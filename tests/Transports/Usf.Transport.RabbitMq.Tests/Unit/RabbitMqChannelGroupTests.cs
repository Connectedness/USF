using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
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
                           .WithSerializer<CloudEventMessageSerializer>()
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
    public async Task RabbitMqChannelPool_ReusesRecoveredIdleChannelWrapper()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var createCallCount = 0;
        await using var pool = new DefaultRabbitMqChannelPool(
            1,
            _ =>
            {
                createCallCount++;
                return Task.FromResult(channel.Object);
            }
        );

        IChannel firstChannel;
        await using (var lease = await pool.AcquireAsync(cancellationToken))
        {
            firstChannel = lease.Channel;
        }

        await channel.ShutdownAsync();
        await channel.RaiseRecoveryAsync();

        await using var recoveredLease = await pool.AcquireAsync(cancellationToken);

        recoveredLease.Channel.Should().BeSameAs(firstChannel);
        createCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqChannelPool_DiscardsUnrecoveredIdleChannelWithoutLeakingSlot()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstChannel = new TestRabbitMqChannel();
        var secondChannel = new TestRabbitMqChannel();
        var channels = new Queue<TestRabbitMqChannel>([firstChannel, secondChannel]);
        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channels.Dequeue().Object));

        await using (await pool.AcquireAsync(cancellationToken)) { }

        await firstChannel.ShutdownAsync();

        IChannel replacement;
        await using (var replacementLease = await pool.AcquireAsync(cancellationToken))
        {
            replacement = replacementLease.Channel;
        }

        await using var reusedReplacementLease = await pool.AcquireAsync(cancellationToken);

        replacement.Should().BeSameAs(secondChannel.Object);
        reusedReplacementLease.Channel.Should().BeSameAs(secondChannel.Object);
        firstChannel.DisposeAsyncCallCount.Should().Be(1);
        channels.Should().BeEmpty();
    }

    [Fact]
    public async Task RabbitMqChannelPool_UnsubscribesShutdownAndRecoveryHandlersDuringDisposal()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channel.Object));

        await using (await pool.AcquireAsync(cancellationToken)) { }

        await pool.DisposeAsync();

        channel.ShutdownAsyncAddCallCount.Should().Be(1);
        channel.ShutdownAsyncRemoveCallCount.Should().Be(1);
        channel.RecoveryAsyncAddCallCount.Should().Be(1);
        channel.RecoveryAsyncRemoveCallCount.Should().Be(1);
        channel.DisposeAsyncCallCount.Should().Be(1);
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

    [Fact]
    public void RabbitMqChannelLease_ChannelThrowsClearErrorWhenDefaultConstructed()
    {
        var lease = default(RabbitMqChannelLease);

        Action act = () => _ = lease.Channel;

        var exception = act.Should().Throw<InvalidOperationException>().Which;
        exception.Message.Should().Be("RabbitMqChannelLease must not be the default instance");
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
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false
        );

        var firstPublish = async () => await target.PublishAsync(
            new ValidationMessageA("first"),
            cancellationToken: cancellationToken
        );

        var deliveryException = (await firstPublish.Should().ThrowAsync<MessageDeliveryException>()).Which;
        deliveryException.TargetName.Should().Be("target");
        deliveryException.Reason.Should().Be(expectedReason);
        deliveryException.InnerException.Should().BeSameAs(publishException);
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken: cancellationToken);

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
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false
        );

        var firstPublish = async () => await target.PublishAsync(
            new ValidationMessageA("first"),
            cancellationToken: cancellationToken
        );

        await firstPublish.Should().ThrowAsync<InvalidOperationException>();
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken: cancellationToken);

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
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false
        );

        var firstPublish = async () => await target.PublishAsync(
            new ValidationMessageA("first"),
            cancellationToken: cancellationToken
        );

        var deliveryException = (await firstPublish.Should().ThrowAsync<MessageDeliveryException>()).Which;
        deliveryException.TargetName.Should().Be("target");
        deliveryException.Reason.Should().Be(MessageDeliveryFailureReason.Timeout);
        deliveryException.InnerException.Should().BeNull();

        channel.BasicPublishAsyncHandler = _ => default;
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken: cancellationToken);

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
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false
        );
        using var cancellationTokenSource = new CancellationTokenSource();

        var publish = target.PublishAsync(
            new ValidationMessageA("first"),
            cancellationToken: cancellationTokenSource.Token
        );
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
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
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
    public async Task RabbitMqOutboundTarget_UsesCallerRoutingKeyForTypedPublish()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqDirectOutboundTarget<ValidationMessageA>(
            "target",
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false,
            "target.route",
            null
        );

        await target.PublishAsync(
            new ValidationMessageA("value"),
            "caller.route",
            cancellationToken: cancellationToken
        );

        channel.LastPublishedRoutingKey.Should().Be("caller.route");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task RabbitMqOutboundTarget_UsesTargetRoutingKeyWhenCallerRoutingKeyIsBlank(
        string? callerRoutingKey
    )
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqDirectOutboundTarget<ValidationMessageA>(
            "target",
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false,
            "target.route",
            null
        );

        await target.PublishAsync(
            new ValidationMessageA("value"),
            callerRoutingKey,
            cancellationToken: cancellationToken
        );

        channel.LastPublishedRoutingKey.Should().Be("target.route");
    }

    [Fact]
    public async Task RabbitMqOutboundTarget_DoesNotEvaluateMessageRoutingKeyFactoryWhenCallerRoutingKeyIsProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var factoryCallCount = 0;
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqTopicOutboundTarget<ValidationMessageA>(
            "target",
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false,
            null,
            _ =>
            {
                factoryCallCount++;
                throw new InvalidOperationException("The routing-key factory should not run.");
            }
        );

        await target.PublishAsync(
            new ValidationMessageA("value"),
            "caller.topic",
            cancellationToken: cancellationToken
        );

        channel.LastPublishedRoutingKey.Should().Be("caller.topic");
        factoryCallCount.Should().Be(0);
    }

    [Fact]
    public async Task RabbitMqOutboundTarget_UsesMessageRoutingKeyFactoryWhenCallerRoutingKeyIsBlank()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var factoryCallCount = 0;
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqTopicOutboundTarget<ValidationMessageA>(
            "target",
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false,
            null,
            message =>
            {
                factoryCallCount++;
                return $"message.{message.Value}";
            }
        );

        await target.PublishAsync(
            new ValidationMessageA("created"),
            "   ",
            cancellationToken: cancellationToken
        );

        channel.LastPublishedRoutingKey.Should().Be("message.created");
        factoryCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqOutboundTarget_BindsCloudEventExtensionsToPrefixedApplicationHeaders()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        CloudEventEnvelope envelope = new (
            "1.0",
            "93f0208d-10fe-47fc-a3e4-daed821f80b7",
            "/tests/rabbitmq",
            "tests.extension",
            new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero),
            null,
            "application/custom",
            null,
            "body"u8.ToArray(),
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            }
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            new FixedEnvelopeSerializer(envelope),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false
        );

        await target.PublishAsync(new ValidationMessageA("value"), cancellationToken: cancellationToken);

        channel.LastPublishedProperties.Should().NotBeNull();
        channel.LastPublishedProperties!.ContentType.Should().Be("application/custom");
        channel.LastPublishedProperties.MessageId.Should().Be(envelope.Id);
        channel.LastPublishedProperties.Headers.Should().NotBeNull();
        channel.LastPublishedProperties.Headers.Should().ContainKey("cloudEvents:id")
           .WhoseValue.Should()
           .Be(envelope.Id);
        channel.LastPublishedProperties.Headers.Should().ContainKey("cloudEvents:traceparent")
           .WhoseValue.Should()
           .Be("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        channel.LastPublishedBody.ToArray().Should().Equal("body"u8.ToArray());
    }

    [Fact]
    public async Task PublishMessageAsync_InjectsProducerTraceContextAfterRouteHeaders()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqHeadersOutboundTarget<ValidationMessageA>(
            "target",
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false,
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["traceparent"] = "route-traceparent",
                ["tracestate"] = "route-tracestate",
                ["baggage"] = "route-baggage",
                ["tenant"] = "tenant-route"
            }
        );
        var publisher = new MessagePublisher(
            new OutboundTopology(
                new Dictionary<Type, OutboundTarget>(),
                new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            )
        );
        Activity? producerActivity = null;
        using var parentActivity = new Activity("parent").SetIdFormat(ActivityIdFormat.W3C);
        parentActivity.TraceStateString = "vendor=value";
        parentActivity.AddBaggage("tenant", "tenant-activity").Start();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == OutboundDiagnostics.ActivitySourceName,
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                if (activity.OperationName == "usf.outbound.publish" &&
                    activity.TraceId == parentActivity.TraceId &&
                    activity.ParentSpanId == parentActivity.SpanId)
                {
                    producerActivity = activity;
                }
            }
        };
        ActivitySource.AddActivityListener(listener);

        await publisher.PublishMessageAsync(
            new ValidationMessageA("value"),
            target,
            cancellationToken: cancellationToken
        );

        producerActivity.Should().NotBeNull();
        producerActivity!.Kind.Should().Be(ActivityKind.Producer);
        producerActivity.ParentSpanId.Should().Be(parentActivity.SpanId);
        channel.LastPublishedProperties.Should().NotBeNull();
        channel.LastPublishedProperties!.Headers.Should().NotBeNull();
        var headers = channel.LastPublishedProperties.Headers!;
        headers.Should().ContainKey("traceparent").WhoseValue.Should().Be(producerActivity.Id);
        headers.Should().ContainKey("tracestate").WhoseValue.Should().Be("vendor=value");
        headers.Should().ContainKey("baggage");
        headers["baggage"].Should().BeOfType<string>()
           .Which.Replace(" ", string.Empty).Should().Be("tenant=tenant-activity");
        headers.Should().ContainKey("tenant").WhoseValue.Should().Be("tenant-route");
        headers.Should().NotContainKey("cloudEvents:traceparent");
    }

    [Fact]
    public async Task PublishRawAsync_DoesNotInjectCurrentTraceContext()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var channelGroup = new RabbitMqChannelGroup(
            "group",
            1,
            _ => Task.FromResult(channel.Object)
        );
        var target = new RabbitMqFanoutOutboundTarget<ValidationMessageA>(
            "target",
            RabbitMqCloudEventsTestFactory.CreateSerializer(),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            TopologyName.Default,
            channelGroup,
            "exchange",
            false
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
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["tenant"] = "tenant-7"
            },
            null,
            null
        );
        using var activity = new Activity("raw")
           .SetIdFormat(ActivityIdFormat.W3C)
           .Start();

        await publisher.PublishRawAsync(message, target, cancellationToken);

        channel.LastPublishedProperties.Should().NotBeNull();
        channel.LastPublishedProperties!.Headers.Should().ContainKey("tenant")
           .WhoseValue.Should()
           .Be("tenant-7");
        channel.LastPublishedProperties.Headers.Should().NotContainKey("traceparent");
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
    public async Task RabbitMqConnectionProvider_DisposeAsyncWaitsForInFlightConnectionRequest()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection();
        using ManualResetEventSlim factoryEntered = new ();
        using ManualResetEventSlim allowFactoryReturn = new ();
        var provider = new RabbitMqConnectionProvider(
            _ =>
            {
                factoryEntered.Set();
                allowFactoryReturn.Wait(cancellationToken);
                return Task.FromResult(connection.Object);
            }
        );
        var first = Task.Run(
            async () => await provider.GetConnectionAsync(cancellationToken),
            cancellationToken
        );

        try
        {
            factoryEntered.Wait(cancellationToken);
            var disposeTask = provider.DisposeAsync().AsTask();

            disposeTask.IsCompleted.Should().BeFalse();
            allowFactoryReturn.Set();
            var resolved = await first;
            await disposeTask;
            Func<Task> getAfterDispose = async () => await provider.GetConnectionAsync(cancellationToken);

            resolved.Should().BeSameAs(connection.Object);
            connection.DisposeAsyncCallCount.Should().Be(1);
            var exception = (await getAfterDispose.Should().ThrowAsync<ObjectDisposedException>()).Which;
            exception.ObjectName.Should().Be(nameof(RabbitMqConnectionProvider));
        }
        finally
        {
            allowFactoryReturn.Set();
        }
    }

    [Fact]
    public async Task RabbitMqConnectionProvider_SynchronousDisposeWaitsForInFlightConnectionRequest()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection();
        using ManualResetEventSlim factoryEntered = new ();
        using ManualResetEventSlim allowFactoryReturn = new ();
        using ManualResetEventSlim disposeStarted = new ();
        var provider = new RabbitMqConnectionProvider(
            _ =>
            {
                factoryEntered.Set();
                allowFactoryReturn.Wait(cancellationToken);
                return Task.FromResult(connection.Object);
            }
        );
        var first = Task.Run(
            async () => await provider.GetConnectionAsync(cancellationToken),
            cancellationToken
        );

        try
        {
            factoryEntered.Wait(cancellationToken);
            var disposeTask = Task.Run(
                () =>
                {
                    disposeStarted.Set();
                    provider.Dispose();
                },
                cancellationToken
            );
            disposeStarted.Wait(cancellationToken);

            disposeTask.IsCompleted.Should().BeFalse();
            allowFactoryReturn.Set();
            var resolved = await first;
            await disposeTask;
            Func<Task> getAfterDispose = async () => await provider.GetConnectionAsync(cancellationToken);

            resolved.Should().BeSameAs(connection.Object);
            connection.DisposeCallCount.Should().Be(1);
            var exception = (await getAfterDispose.Should().ThrowAsync<ObjectDisposedException>()).Which;
            exception.ObjectName.Should().Be(nameof(RabbitMqConnectionProvider));
        }
        finally
        {
            allowFactoryReturn.Set();
        }
    }

    [Fact]
    public async Task RabbitMqOutboundTopology_RejectsFactoryWithAutomaticRecoveryDisabledWhenConnectionIsFirstCreated()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var createFactoryCallCount = 0;
        var builder = new RabbitMqOutboundTopologyBuilder();
        builder.UseConnectionFactory(
            _ =>
            {
                createFactoryCallCount++;
                return new ConnectionFactory
                {
                    AutomaticRecoveryEnabled = false
                };
            }
        );
        var services = new ServiceCollection();
        services
           .AddUsf()
           .AddRabbitMqOutboundTopology(
                _ =>
                {
                    foreach (var channelGroup in builder.Build().ChannelGroups)
                    {
                        _.ChannelGroup(
                            channelGroup.Name,
                            channelGroup.MaximumChannelCount,
                            channelGroup.PublisherConfirmMode,
                            channelGroup.PublisherConfirmTimeout
                        );
                    }

                    _.UseConnectionFactory(
                        serviceProvider =>
                        {
                            createFactoryCallCount++;
                            return new ConnectionFactory
                            {
                                AutomaticRecoveryEnabled = false
                            };
                        }
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();
        await using var topology = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();

        createFactoryCallCount.Should().Be(0);

        var action = async () => await topology.GetConnectionAsync(cancellationToken);

        var exception = (await action.Should().ThrowAsync<OutboundTopologyValidationException>()).Which;
        exception.ValidationErrors.Should().ContainSingle()
           .Which.Should()
           .Be(
                "RabbitMQ automatic connection recovery must be enabled. Configure ConnectionFactory.AutomaticRecoveryEnabled to true."
            );
        createFactoryCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqConnectionProvider_LogsLifecycleEventsAndKeepsCachedConnection()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var loggerProvider = new RecordingLoggerProvider();
        using var loggerFactory = new RecordingLoggerFactory(loggerProvider);
        var connection = new TestRabbitMqConnection();
        var createConnectionCallCount = 0;
        var provider = new RabbitMqConnectionProvider(
            _ =>
            {
                createConnectionCallCount++;
                return Task.FromResult(connection.Object);
            },
            loggerFactory.CreateLogger(typeof(RabbitMqConnectionProvider))
        );

        var initialConnection = await provider.GetConnectionAsync(cancellationToken);
        await connection.RaiseConnectionShutdownAsync(320, "CONNECTION_FORCED");
        var cachedConnection = await provider.GetConnectionAsync(cancellationToken);
        var recoveryException = new InvalidOperationException("recovery failed");
        await connection.RaiseConnectionRecoveryErrorAsync(recoveryException);
        await connection.RaiseRecoverySucceededAsync();

        initialConnection.Should().BeSameAs(connection.Object);
        cachedConnection.Should().BeSameAs(connection.Object);
        createConnectionCallCount.Should().Be(1);
        connection.ConnectionShutdownAsyncAddCallCount.Should().Be(1);
        connection.RecoverySucceededAsyncAddCallCount.Should().Be(1);
        connection.ConnectionRecoveryErrorAsyncAddCallCount.Should().Be(1);
        var shutdownEntry = loggerProvider.Entries.Should().ContainSingle(
            entry => entry.LogLevel == LogLevel.Warning &&
                     Equals(entry.Fields["Transition"], "shutdown")
        ).Which;
        shutdownEntry.Fields["Initiator"].Should().Be(ShutdownInitiator.Library);
        shutdownEntry.Fields["ReplyCode"].Should().Be((ushort) 320);
        shutdownEntry.Fields["ReplyText"].Should().Be("CONNECTION_FORCED");
        loggerProvider.Entries.Should().ContainSingle(
            entry => entry.LogLevel == LogLevel.Warning &&
                     entry.Exception == recoveryException &&
                     Equals(entry.Fields["Transition"], "recovery-failed")
        );
        loggerProvider.Entries.Should().ContainSingle(
            entry => entry.LogLevel == LogLevel.Information &&
                     Equals(entry.Fields["Transition"], "recovery-succeeded")
        );

        await provider.DisposeAsync();

        connection.ConnectionShutdownAsyncRemoveCallCount.Should().Be(1);
        connection.RecoverySucceededAsyncRemoveCallCount.Should().Be(1);
        connection.ConnectionRecoveryErrorAsyncRemoveCallCount.Should().Be(1);
        connection.DisposeAsyncCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqConnectionProvider_UnsubscribesLifecycleEventsDuringSynchronousDisposal()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var connection = new TestRabbitMqConnection();
        var provider = new RabbitMqConnectionProvider(_ => Task.FromResult(connection.Object));

        _ = await provider.GetConnectionAsync(cancellationToken);

        provider.Dispose();

        connection.ConnectionShutdownAsyncRemoveCallCount.Should().Be(1);
        connection.RecoverySucceededAsyncRemoveCallCount.Should().Be(1);
        connection.ConnectionRecoveryErrorAsyncRemoveCallCount.Should().Be(1);
        connection.DisposeCallCount.Should().Be(1);
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
    public async Task RabbitMqOutboundTopology_ForwardsConfiguredChannelOptions()
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

        CreateChannelOptions options = new (
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true
        );
        await using var first = await topology.CreateChannelAsync(options, cancellationToken);
        await using var second = await topology.CreateChannelAsync(options: null, cancellationToken);

        connection.CreateChannelOptions.Should().HaveCount(2);
        connection.CreateChannelOptions[0].Should().BeSameAs(options);
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
        var configuration = services.Single(
            static descriptor => descriptor.ServiceType == typeof(RabbitMqOutboundTopologyConfiguration)
        );

        Action act = () => _ = Compile((RabbitMqOutboundTopologyConfiguration) configuration.ImplementationInstance!);

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
        var configuration = services.Single(
            static descriptor => descriptor.ServiceType == typeof(RabbitMqOutboundTopologyConfiguration)
        );

        Action act = () => _ = Compile((RabbitMqOutboundTopologyConfiguration) configuration.ImplementationInstance!);

        var exception = act.Should().Throw<OutboundTopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Channel group '$implicit:user-defined' uses reserved name prefix '$implicit:'."
        );
    }

    [Fact]
    public void RabbitMqOutboundTopologyCompiler_RejectsChannelGroupsThatNoTargetReferences()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
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
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();

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
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
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
                           .WithSerializer<CloudEventMessageSerializer>()
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
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
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
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "secondary",
                        target => target
                           .ToFanoutAddress("orders-address")
                           .UseChannelGroup("shared")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "tertiary",
                        target => target
                           .ToFanoutAddress("orders-address")
                           .UseChannelGroup("shared")
                           .WithSerializer<CloudEventMessageSerializer>()
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
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Address("orders-address", "orders");
                    builder.Publish<ValidationMessageA>(
                        target => target.ToFanoutAddress("orders-address").WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "secondary",
                        target => target.ToFanoutAddress("orders-address").WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "tertiary",
                        target => target.ToFanoutAddress("orders-address").WithSerializer<CloudEventMessageSerializer>()
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
        RabbitMqChannelSource channelSource = new (connectionProvider);
        channelSource.SetChannelBudget(worstCaseChannelCount, description);

        return new RabbitMqOutboundTopology(
            new OutboundTopology(
                new Dictionary<Type, OutboundTarget>(),
                new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            ),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            Array.Empty<RabbitMqExchangeDefinition>(),
            Array.Empty<RabbitMqQueueDefinition>(),
            Array.Empty<RabbitMqBindingDefinition>(),
            Array.Empty<RabbitMqAddressDefinition>(),
            channelGroups,
            Array.Empty<OutboundTarget>(),
            connectionProvider,
            channelSource
        );
    }

    private static RabbitMqOutboundTopology Compile(RabbitMqOutboundTopologyConfiguration configuration)
    {
        RabbitMqConnectionProvider connectionProvider = new (
            _ => throw new InvalidOperationException("The validation test should not open a RabbitMQ connection.")
        );
        RabbitMqOutboundTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            serializerType => serializerType == typeof(CloudEventMessageSerializer) ?
                RabbitMqCloudEventsTestFactory.CreateSerializer() :
                null
        );

        return compiler.Compile(TopologyName.Default, configuration, connectionProvider);
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

    private sealed class FixedEnvelopeSerializer : IMessageSerializer
    {
        private readonly CloudEventEnvelope _envelope;

        public FixedEnvelopeSerializer(CloudEventEnvelope envelope)
        {
            _envelope = envelope;
        }

        public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
            T message,
            in CloudEventMetadata metadata,
            string? type,
            string? dataSchema,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<CloudEventEnvelope>(_envelope);
        }
    }
}
