using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Configuration;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqChannelGroupTests
{
    [Fact]
    public void RabbitMqOutboundTopologyBuilder_UsesImplicitPrivateChannelGroupsByDefault()
    {
        var builder = new RabbitMqOutboundTopologyBuilder();

        builder.UseConnectionFactory(static _ => new ConnectionFactory());

        var configuration = builder.Build();

        configuration.ChannelGroups.Should().BeEmpty();
        configuration.DefaultPublisherConfirmMode.Should().Be(RabbitMqPublisherConfirmMode.Confirms);
        configuration.DefaultPublisherConfirmTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void RabbitMqOutboundTopologyBuilder_AllowsPublisherConfirmDefaultsAndChannelGroupModeToBeOverridden()
    {
        var builder = new RabbitMqOutboundTopologyBuilder();

        builder
           .WithDefaultPublisherConfirmMode(RabbitMqPublisherConfirmMode.FireAndForget)
           .WithDefaultPublisherConfirmTimeout(TimeSpan.FromSeconds(7))
           .ChannelGroup("confirmed", 3, RabbitMqPublisherConfirmMode.Confirms, TimeSpan.FromSeconds(11));

        var configuration = builder.Build();

        configuration.DefaultPublisherConfirmMode.Should().Be(RabbitMqPublisherConfirmMode.FireAndForget);
        configuration.DefaultPublisherConfirmTimeout.Should().Be(TimeSpan.FromSeconds(7));
        configuration.ChannelGroups.Should().ContainSingle()
           .Which.Should().Be(
                new RabbitMqChannelGroupDefinition(
                    "confirmed",
                    3,
                    RabbitMqPublisherConfirmMode.Confirms,
                    TimeSpan.FromSeconds(11)
                )
            );
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_AppliesTopologyPublisherConfirmDefaultToExplicitChannelGroups()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqOutboundTopology(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.WithDefaultPublisherConfirmMode(RabbitMqPublisherConfirmMode.FireAndForget);
                builder.Exchange("orders", ExchangeType.Fanout);
                builder.Address("orders-address", "orders");
                builder.ChannelGroup("shared", 2);
                builder.Publish<ValidationMessageA>(
                    target => target
                       .ToFanoutAddress("orders-address")
                       .UseChannelGroup("shared")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();

        topology.ChannelGroups.Should().ContainSingle()
           .Which.PublisherConfirmMode.Should().Be(RabbitMqPublisherConfirmMode.FireAndForget);
    }

    [Fact]
    public void RabbitMqOutboundTopologyBuilder_RejectsReservedImplicitChannelGroupNamePrefix()
    {
        var builder = new RabbitMqOutboundTopologyBuilder();

        var action = () => builder.ChannelGroup("$implicit:user-defined", 1);

        action
           .Should().Throw<ArgumentException>()
           .WithParameterName("name")
           .WithMessage("Channel group names beginning with '$implicit:' are reserved.*");
    }

    [Fact]
    public async Task RabbitMqChannelPool_ReusesHealthyChannelsSequentially()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var createdChannels = new List<TestRabbitMqChannel>();
        await using var pool = new DefaultRabbitMqChannelPool(
            1,
            _ =>
            {
                var channel = new TestRabbitMqChannel();
                createdChannels.Add(channel);
                return Task.FromResult(channel.Object);
            }
        );

        IChannel firstChannel;
        await using (var lease = await pool.AcquireAsync(cancellationToken))
        {
            firstChannel = lease.Channel;
        }

        await using var secondLease = await pool.AcquireAsync(cancellationToken);

        secondLease.Channel.Should().BeSameAs(firstChannel);
        createdChannels.Should().HaveCount(1);
    }

    [Fact]
    public async Task RabbitMqChannelPool_WaitsForReturnedChannelsWhenBoundIsReached()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channel.Object));

        var firstLease = await pool.AcquireAsync(cancellationToken);
        var waitingAcquire = pool.AcquireAsync(cancellationToken).AsTask();

        waitingAcquire.IsCompleted.Should().BeFalse();

        await firstLease.DisposeAsync();
        await using var secondLease = await waitingAcquire.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

        secondLease.Channel.Should().BeSameAs(channel.Object);
    }

    [Fact]
    public async Task RabbitMqChannelPool_AcquiresDistinctChannelsForConcurrentLeases()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var createdChannels = new List<TestRabbitMqChannel>();
        await using var pool = new DefaultRabbitMqChannelPool(
            2,
            _ =>
            {
                var channel = new TestRabbitMqChannel();
                createdChannels.Add(channel);
                return Task.FromResult(channel.Object);
            }
        );

        var firstLeaseTask = pool.AcquireAsync(cancellationToken).AsTask();
        var secondLeaseTask = pool.AcquireAsync(cancellationToken).AsTask();
        var leases = await Task.WhenAll(firstLeaseTask, secondLeaseTask);

        try
        {
            leases[0].Channel.Should().NotBeSameAs(leases[1].Channel);
            createdChannels.Should().HaveCount(2);
        }
        finally
        {
            await leases[0].DisposeAsync();
            await leases[1].DisposeAsync();
        }
    }

    [Fact]
    public async Task RabbitMqChannelPool_ReplacesChannelsThatFaultWhileLeased()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstChannel = new TestRabbitMqChannel();
        var secondChannel = new TestRabbitMqChannel();
        var channels = new Queue<TestRabbitMqChannel>([firstChannel, secondChannel]);
        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channels.Dequeue().Object));

        var lease = await pool.AcquireAsync(cancellationToken);
        var leasedChannel = lease.Channel;
        await firstChannel.ShutdownAsync();
        await lease.DisposeAsync();

        await using var replacementLease = await pool.AcquireAsync(cancellationToken);

        replacementLease.Channel.Should().NotBeSameAs(leasedChannel);
        replacementLease.Channel.Should().BeSameAs(secondChannel.Object);
        firstChannel.DisposeAsyncCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqChannelPool_PropagatesCancellationWhileWaitingForReturnedChannel()
    {
        var channel = new TestRabbitMqChannel();
        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channel.Object));
        await using var holdingLease = await pool.AcquireAsync(TestContext.Current.CancellationToken);

        using var cancellationTokenSource = new CancellationTokenSource();
        var waitingAcquire = pool.AcquireAsync(cancellationTokenSource.Token).AsTask();

        waitingAcquire.IsCompleted.Should().BeFalse();
        await cancellationTokenSource.CancelAsync();

        Func<Task> action = async () => await waitingAcquire;
        await action.Should().ThrowAsync<OperationCanceledException>();
    }

    [Theory]
    [InlineData(false, MessageDeliveryFailureReason.Nacked)]
    [InlineData(true, MessageDeliveryFailureReason.Returned)]
    public async Task RabbitMqOutboundTarget_ReusesChannelWhenBrokerRejectsPublishButChannelStaysOpen(
        bool isReturn,
        MessageDeliveryFailureReason expectedReason
    )
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var publishException = new PublishException(1, isReturn);
        var firstAttempt = true;
        channel.BasicPublishAsyncHandler = _ =>
        {
            if (firstAttempt)
            {
                firstAttempt = false;
                throw publishException;
            }

            return default;
        };

        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            channelGroup,
            "exchange",
            false
        );

        var firstPublish = async () => await target.PublishAsync(new ValidationMessageA("first"), cancellationToken);

        var deliveryException = (await firstPublish.Should().ThrowAsync<MessageDeliveryException>()).Which;
        deliveryException.TargetName.Should().Be("target");
        deliveryException.Reason.Should().Be(expectedReason);
        deliveryException.InnerException.Should().BeSameAs(publishException);
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken);

        channel.BasicPublishCallCount.Should().Be(2);
        channel.DisposeAsyncCallCount.Should().Be(0);
    }

    [Fact]
    public async Task RabbitMqOutboundTarget_ReleasesLeaseSlotWhenPublishFaultsAndClosesChannel()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstChannel = new TestRabbitMqChannel();
        var secondChannel = new TestRabbitMqChannel();
        var channels = new Queue<TestRabbitMqChannel>([firstChannel, secondChannel]);
        firstChannel.BasicPublishAsyncHandler = async _ =>
        {
            await firstChannel.ShutdownAsync().ConfigureAwait(false);
            throw new InvalidOperationException("Publish failed.");
        };

        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channels.Dequeue().Object)
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            channelGroup,
            "exchange",
            false
        );

        var firstPublish = async () => await target.PublishAsync(new ValidationMessageA("first"), cancellationToken);

        await firstPublish.Should().ThrowAsync<InvalidOperationException>();
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken);

        firstChannel.DisposeAsyncCallCount.Should().Be(1);
        secondChannel.BasicPublishCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqOutboundTarget_MapsTrackedPublishTimeoutAndReusesChannel()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel
        {
            BasicPublishAsyncHandler = token => new ValueTask(Task.Delay(Timeout.InfiniteTimeSpan, token))
        };
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object),
            publisherConfirmTimeout: TimeSpan.FromMilliseconds(20)
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            channelGroup,
            "exchange",
            false
        );

        var firstPublish = async () => await target.PublishAsync(new ValidationMessageA("first"), cancellationToken);

        var deliveryException = (await firstPublish.Should().ThrowAsync<MessageDeliveryException>()).Which;
        deliveryException.TargetName.Should().Be("target");
        deliveryException.Reason.Should().Be(MessageDeliveryFailureReason.Timeout);
        deliveryException.InnerException.Should().BeNull();

        channel.BasicPublishAsyncHandler = _ => default;
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken);

        channel.BasicPublishCallCount.Should().Be(2);
        channel.DisposeAsyncCallCount.Should().Be(0);
    }

    [Fact]
    public async Task RabbitMqOutboundTarget_PropagatesCallerCancellationWithoutWrapping()
    {
        var channel = new TestRabbitMqChannel();
        channel.BasicPublishAsyncHandler =
            token => new ValueTask(Task.Delay(Timeout.InfiniteTimeSpan, token));
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object),
            publisherConfirmTimeout: TimeSpan.FromSeconds(5)
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            channelGroup,
            "exchange",
            false
        );
        using var cancellationTokenSource = new CancellationTokenSource();

        var publish = target.PublishAsync(new ValidationMessageA("first"), cancellationTokenSource.Token);
        await cancellationTokenSource.CancelAsync();

        var action = async () => await publish;
        await action.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task PublishRawAsync_MapsBrokerReturnToMessageDeliveryException()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var publishException = new PublishException(1, isReturn: true);
        channel.BasicPublishAsyncHandler = _ => throw publishException;
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            channelGroup,
            "exchange",
            true
        );
        var publisher = new MessagePublisher(
            new OutboundTopology(
                new Dictionary<Type, OutboundTarget>(),
                new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            )
        );
        SerializedMessage message = new (
            "body"u8.ToArray(),
            null,
            null,
            new Dictionary<string, string?>(StringComparer.Ordinal),
            null,
            null
        );

        var action = async () => await publisher.PublishRawAsync(message, target, cancellationToken);

        var deliveryException = (await action.Should().ThrowAsync<MessageDeliveryException>()).Which;
        deliveryException.TargetName.Should().Be("target");
        deliveryException.Reason.Should().Be(MessageDeliveryFailureReason.Returned);
        deliveryException.InnerException.Should().BeSameAs(publishException);
    }

    [Fact]
    public async Task RabbitMqConnectionProvider_CreatesOnlyOneConnectionForConcurrentRequests()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection();
        var createConnection =
            new TaskCompletionSource<IConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var createCallCount = 0;
        var provider = new RabbitMqConnectionProvider(
            _ =>
            {
                Interlocked.Increment(ref createCallCount);
                return createConnection.Task;
            }
        );

        var first = provider.GetConnectionAsync(cancellationToken);
        var second = provider.GetConnectionAsync(cancellationToken);
        createConnection.SetResult(connection.Object);
        var resolved = await Task.WhenAll(first, second);

        resolved.Should().OnlyContain(resolvedConnection => ReferenceEquals(resolvedConnection, connection.Object));
        createCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqConnectionProvider_RetriesAfterAFailedConnectionAttempt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection();
        var attempt = 0;
        await using var provider = new RabbitMqConnectionProvider(
            _ =>
            {
                attempt++;
                return attempt == 1 ?
                    Task.FromException<IConnection>(new InvalidOperationException("broker not ready")) :
                    Task.FromResult(connection.Object);
            }
        );

        // ReSharper disable once AccessToDisposedClosure -- firstAttempt is called before disposal
        var firstAttempt = async () => await provider.GetConnectionAsync(cancellationToken);
        await firstAttempt.Should().ThrowAsync<InvalidOperationException>();

        var resolved = await provider.GetConnectionAsync(cancellationToken);

        resolved.Should().BeSameAs(connection.Object);
        attempt.Should().Be(2);
    }

    [Fact]
    public async Task RabbitMqOutboundTopology_ThrowsWhenWorstCaseChannelCountExceedsBrokerLimit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection
        {
            ChannelMax = 3
        };
        await using var topology = CreateTopology(
            new RabbitMqConnectionProvider(_ => Task.FromResult(connection.Object)),
            Array.Empty<RabbitMqChannelGroup>(),
            4,
            "2 channel groups"
        );

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Func<Task> act = async () => await topology.GetConnectionAsync(cancellationToken);

        var exception = (await act.Should().ThrowAsync<OutboundTopologyValidationException>()).Which;
        exception.ValidationErrors.Should().HaveCount(1);
        exception
           .ValidationErrors[0]
           .Should()
           .Be(
                "RabbitMQ outbound topology may open up to 4 channels (2 channel groups), but the broker negotiated channel_max=3."
            );
        connection.DisposeAsyncCallCount.Should().Be(0);
    }

    [Fact]
    public async Task RabbitMqOutboundTopology_SkipsChannelLimitCheckWhenBrokerAdvertisesUnlimitedChannels()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection
        {
            ChannelMax = 0
        };
        await using var topology = CreateTopology(
            new RabbitMqConnectionProvider(_ => Task.FromResult(connection.Object)),
            Array.Empty<RabbitMqChannelGroup>(),
            50,
            "channel group 'shared' max 50"
        );

        var resolved = await topology.GetConnectionAsync(cancellationToken);

        resolved.Should().BeSameAs(connection.Object);
        connection.DisposeAsyncCallCount.Should().Be(0);
    }

    [Fact]
    public async Task RabbitMqOutboundTopology_ConfiguresTrackedChannelsOnlyForConfirmsMode()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var trackedChannel = new TestRabbitMqChannel();
        var fireAndForgetChannel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(trackedChannel.Object);
        connection.EnqueueChannel(fireAndForgetChannel.Object);
        await using var topology = CreateTopology(
            new RabbitMqConnectionProvider(_ => Task.FromResult(connection.Object)),
            Array.Empty<RabbitMqChannelGroup>(),
            0,
            "no channel groups"
        );

        await using var first = await topology.CreateChannelAsync(
            RabbitMqPublisherConfirmMode.Confirms,
            cancellationToken
        );
        await using var second = await topology.CreateChannelAsync(
            RabbitMqPublisherConfirmMode.FireAndForget,
            cancellationToken
        );

        connection.CreateChannelOptions.Should().HaveCount(2);
        connection.CreateChannelOptions[0].Should().NotBeNull();
        connection.CreateChannelOptions[0]!.PublisherConfirmationsEnabled.Should().BeTrue();
        connection.CreateChannelOptions[0]!.PublisherConfirmationTrackingEnabled.Should().BeTrue();
        connection.CreateChannelOptions[1].Should().BeNull();
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_RejectsInvalidChannelGroupSizes()
    {
        var services = new ServiceCollection();
        services.AddSingleton(
            new RabbitMqOutboundTopologyConfiguration(
                static _ => new ConnectionFactory(),
                Array.Empty<RabbitMqExchangeDefinition>(),
                Array.Empty<RabbitMqQueueDefinition>(),
                Array.Empty<RabbitMqBindingDefinition>(),
                Array.Empty<RabbitMqAddressDefinition>(),
                [new RabbitMqChannelGroupDefinition("invalid", 0)],
                Array.Empty<RabbitMqOutboundTargetDefinition>()
            )
        );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = RabbitMqOutboundTopologyCompiler.Compile(serviceProvider);

        var exception = act.Should().Throw<OutboundTopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Channel group 'invalid' maximum channel count must be greater than zero."
        );
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_RejectsReservedImplicitChannelGroupNamePrefix()
    {
        var services = new ServiceCollection();
        services.AddSingleton(
            new RabbitMqOutboundTopologyConfiguration(
                static _ => new ConnectionFactory(),
                Array.Empty<RabbitMqExchangeDefinition>(),
                Array.Empty<RabbitMqQueueDefinition>(),
                Array.Empty<RabbitMqBindingDefinition>(),
                Array.Empty<RabbitMqAddressDefinition>(),
                [new RabbitMqChannelGroupDefinition("$implicit:user-defined", 1)],
                Array.Empty<RabbitMqOutboundTargetDefinition>()
            )
        );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = RabbitMqOutboundTopologyCompiler.Compile(serviceProvider);

        var exception = act.Should().Throw<OutboundTopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Channel group '$implicit:user-defined' uses reserved name prefix '$implicit:'."
        );
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_RejectsChannelGroupsThatNoTargetReferences()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqOutboundTopology(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.Exchange("orders", ExchangeType.Fanout);
                builder.Address("orders-address", "orders");
                builder.ChannelGroup("referenced", 2);
                builder.ChannelGroup("orphaned", 5);
                builder.Publish<ValidationMessageA>(
                    target => target
                       .ToFanoutAddress("orders-address")
                       .UseChannelGroup("referenced")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = RabbitMqOutboundTopologyCompiler.Compile(serviceProvider);

        var exception = act.Should().Throw<OutboundTopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle()
           .Which.Should().Be("Channel group 'orphaned' is configured but no outbound target references it.");
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_LogsWorstCaseChannelCountAtCompileTime()
    {
        var loggerProvider = new RecordingLoggerProvider();
        using var loggerFactory = new RecordingLoggerFactory(loggerProvider);
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(loggerFactory);
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqOutboundTopology(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.Exchange("orders", ExchangeType.Fanout);
                builder.Address("orders-address", "orders");
                builder.ChannelGroup("shared", 11);
                builder.Publish<ValidationMessageA>(
                    target => target
                       .ToFanoutAddress("orders-address")
                       .UseChannelGroup("shared")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        _ = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();

        loggerProvider.Entries.Should().Contain(
            entry => entry.LogLevel == LogLevel.Information &&
                     entry.Message ==
                     "RabbitMQ outbound topology may open up to 11 channels (channel group 'shared' max 11)"
        );
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_AssignsExplicitChannelGroupToEveryReferencingTarget()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqOutboundTopology(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.Exchange("orders", ExchangeType.Fanout);
                builder.Address("orders-address", "orders");
                builder.ChannelGroup("shared", 2);
                builder.Publish<ValidationMessageA>(
                    target => target
                       .ToFanoutAddress("orders-address")
                       .UseChannelGroup("shared")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "secondary",
                    target => target
                       .ToFanoutAddress("orders-address")
                       .UseChannelGroup("shared")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "tertiary",
                    target => target
                       .ToFanoutAddress("orders-address")
                       .UseChannelGroup("shared")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();
        var channelGroups = topology.Targets.Select(GetChannelGroup).Distinct().ToList();

        topology.Targets.Should().HaveCount(3);
        channelGroups.Should().ContainSingle("all targets intentionally reference the same channel group");
        channelGroups[0].Name.Should().Be("shared");
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_CreatesImplicitPrivateSingleChannelGroupsPerTarget()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqOutboundTopology(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.Exchange("orders", ExchangeType.Fanout);
                builder.Address("orders-address", "orders");
                builder.Publish<ValidationMessageA>(
                    target => target.ToFanoutAddress("orders-address").WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "secondary",
                    target => target.ToFanoutAddress("orders-address").WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "tertiary",
                    target => target.ToFanoutAddress("orders-address").WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();
        var channelGroups = topology.Targets.Select(GetChannelGroup).ToList();

        topology.Targets.Should().HaveCount(3);
        topology.ChannelGroups.Should().HaveCount(3);
        topology.ChannelGroups.Should().OnlyContain(channelGroup => channelGroup.MaximumChannelCount == 1);
        topology.ChannelGroups.Should().OnlyContain(
            channelGroup => channelGroup.PublisherConfirmMode == RabbitMqPublisherConfirmMode.Confirms
        );
        topology.ChannelGroups.Should().OnlyContain(
            channelGroup => channelGroup.PublisherConfirmTimeout == TimeSpan.FromSeconds(30)
        );
        channelGroups.Distinct().Should().HaveCount(3);
    }

    [Fact]
    public async Task RabbitMqOutboundTopology_DisposesChannelGroupsBeforeConnectionProvider()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var disposalEvents = new List<string>();
        var channel = new TestRabbitMqChannel(disposalEvents, "channel-group");
        var connection = new TestRabbitMqConnection(disposalEvents);
        var channelGroup = new RabbitMqChannelGroup("group", 1, _ => Task.FromResult(channel.Object));
        var topology = CreateTopology(
            new RabbitMqConnectionProvider(_ => Task.FromResult(connection.Object)),
            [channelGroup],
            1,
            "channel group 'group' max 1"
        );

        await using (await channelGroup.AcquireAsync(cancellationToken)) { }

        _ = await topology.GetConnectionAsync(cancellationToken);

        await topology.DisposeAsync();

        disposalEvents.Should().Equal("channel-group", "connection");
    }

    private static RabbitMqOutboundTopology CreateTopology(
        RabbitMqConnectionProvider connectionProvider,
        IReadOnlyList<RabbitMqChannelGroup> channelGroups,
        int worstCaseChannelCount,
        string description
    )
    {
        return new RabbitMqOutboundTopology(
            new OutboundTopology(
                new Dictionary<Type, OutboundTarget>(),
                new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            ),
            Array.Empty<RabbitMqExchangeDefinition>(),
            Array.Empty<RabbitMqQueueDefinition>(),
            Array.Empty<RabbitMqBindingDefinition>(),
            Array.Empty<RabbitMqAddressDefinition>(),
            channelGroups,
            Array.Empty<OutboundTarget>(),
            connectionProvider,
            worstCaseChannelCount,
            description
        );
    }

    private static RabbitMqChannelGroup GetChannelGroup(OutboundTarget target)
    {
        var rabbitMqTargetType = target.GetType();
        while (rabbitMqTargetType is not null &&
               (!rabbitMqTargetType.IsGenericType ||
                rabbitMqTargetType.GetGenericTypeDefinition() != typeof(RabbitMqOutboundTarget<>)))
        {
            rabbitMqTargetType = rabbitMqTargetType.BaseType;
        }

        var field = rabbitMqTargetType!.GetField("_channelGroup", BindingFlags.Instance | BindingFlags.NonPublic);
        return (RabbitMqChannelGroup) field!.GetValue(target)!;
    }
}
