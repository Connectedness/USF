using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class AddRabbitMqInboundTopologyTests
{
    [Fact]
    public void AddRabbitMqInboundTopology_AllowsDefaultInboundAndOutboundTopologiesIndependently()
    {
        var services = new ServiceCollection();
        services.AddScoped<IMessageHandler<ValidationMessageA>, ValidationMessageAHandler>();
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("outbound", ExchangeType.Fanout);
                    builder.Address("outbound-address", "outbound");
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutAddress("outbound-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            )
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<IOutboundTopologyRegistry>()
           .Names.Should().ContainSingle().Which.Should().Be(TopologyName.Default);
        serviceProvider.GetRequiredService<IInboundTopologyRegistry>()
           .Names.Should().ContainSingle().Which.Should().Be(TopologyName.Default);
        serviceProvider.GetRequiredService<IOutboundTopology>()
           .GetRequiredTarget<ValidationMessageA>()
           .TopologyName.Should().Be(TopologyName.Default);
        serviceProvider.GetRequiredService<IInboundTopology>()
           .Endpoints.Should().ContainSingle()
           .Which.TopologyName.Should().Be(TopologyName.Default);
    }

    [Fact]
    public void AddRabbitMqInboundTopology_RejectsDuplicateInboundTopologyNames()
    {
        var services = new ServiceCollection();
        var builder = services.AddUsf();
        builder.AddRabbitMqInboundTopology("shared", static _ => { });

        var action = () => builder.AddRabbitMqInboundTopology("shared", static _ => { });

        action.Should().Throw<InvalidOperationException>()
           .WithMessage("Inbound topology 'shared' is already registered. Registered inbound topologies: shared.");
    }

    [Fact]
    public void InboundTopologyRegistry_ResolvesNamedTopologiesIndependently()
    {
        TopologyName firstTopologyName = new ("first");
        TopologyName secondTopologyName = new ("second");
        var services = new ServiceCollection();
        services.AddScoped<IMessageHandler<ValidationMessageA>, ValidationMessageAHandler>();
        services.AddScoped<IMessageHandler<ValidationMessageB>, ValidationMessageBHandler>();
        services.AddTestCloudEvents()
           .AddRabbitMqInboundTopology(
                firstTopologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("first-queue");
                    builder.Consume(
                        "first-queue",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            )
           .AddRabbitMqInboundTopology(
                secondTopologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("second-queue");
                    builder.Consume(
                        "second-queue",
                        endpoint => endpoint.Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var registry = serviceProvider.GetRequiredService<IInboundTopologyRegistry>();

        registry.Names.Should().BeEquivalentTo([firstTopologyName, secondTopologyName]);
        registry.GetRequiredTopology(firstTopologyName)
           .Endpoints.Should().ContainSingle()
           .Which.MessageType.Should().Be(typeof(ValidationMessageA));
        registry.GetRequiredTopology(secondTopologyName)
           .Endpoints.Should().ContainSingle()
           .Which.MessageType.Should().Be(typeof(ValidationMessageB));
    }


    [Fact]
    public void AddRabbitMqInboundTopology_CompilesDispatchIndexForInboundAliases()
    {
        var services = new ServiceCollection();
        services.AddScoped<IMessageHandler<ValidationMessageA>, ValidationMessageAHandler>();
        services
           .AddUsf()
           .UseCloudEvents(options => options.Source = "/tests/rabbitmq")
           .MapMessageContracts(
                contracts => contracts.Map<ValidationMessageA>("tests.current").WithInboundAlias("tests.legacy")
            )
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqInboundTopology>();

        topology.TryDispatch("inbound", "tests.current", out var currentEndpoint).Should().BeTrue();
        topology.TryDispatch("inbound", "tests.legacy", out var legacyEndpoint).Should().BeTrue();
        currentEndpoint.Should().BeSameAs(legacyEndpoint);
        currentEndpoint!.Name.Should().Be("inbound:tests.current");
    }

    [Fact]
    public void Compile_RejectsMissingHandlerService()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<IInboundTopology>();

        var exception = action.Should().Throw<InboundTopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Inbound handler service 'Usf.Core.Messaging.IMessageHandler`1[Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA]' for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' is not registered."
        );
    }

    [Fact]
    public async Task CreateChannelAsync_RejectsInboundConnectionFactoryWithoutTopologyRecovery()
    {
        var services = new ServiceCollection();
        services.AddScoped<IMessageHandler<ValidationMessageA>, ValidationMessageAHandler>();
        services.AddTestCloudEvents()
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(
                        static _ => new ConnectionFactory
                        {
                            AutomaticRecoveryEnabled = true,
                            TopologyRecoveryEnabled = false
                        }
                    );
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        await using var serviceProvider = services.BuildServiceProvider();
        var topology = serviceProvider.GetRequiredService<RabbitMqInboundTopology>();

        var action = async () => await topology.CreateChannelAsync(TestContext.Current.CancellationToken);

        var exception = await action.Should().ThrowAsync<InboundTopologyValidationException>();
        exception.Which.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "RabbitMQ topology recovery must be enabled for inbound topologies so RabbitMQ.Client can recover consumer subscriptions. Configure ConnectionFactory.TopologyRecoveryEnabled to true."
        );
    }

    [Fact]
    public void AddRabbitMqInboundTopology_AppliesCustomInspectorAndChannelGroupKnobs()
    {
        var services = new ServiceCollection();
        services.AddScoped<IMessageHandler<ValidationMessageA>, ValidationMessageAHandler>();
        services.AddSingleton<RawInspector>();
        services.AddTestCloudEvents()
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.ChannelGroup(
                        "shared",
                        maximumChannelCount: 3,
                        prefetchCount: 7,
                        consumerDispatchConcurrency: 2
                    );
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .UseInspector<RawInspector>()
                           .UseChannelGroup("shared")
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var endpoint = serviceProvider.GetRequiredService<RabbitMqInboundTopology>()
           .Endpoints.Should().ContainSingle().Which;

        endpoint.InspectorType.Should().Be(typeof(RawInspector));
        endpoint.ChannelGroup.Name.Should().Be("shared");
        endpoint.ChannelGroup.MaximumChannelCount.Should().Be(3);
        endpoint.ChannelGroup.PrefetchCount.Should().Be(7);
        endpoint.ChannelGroup.ConsumerDispatchConcurrency.Should().Be(2);
    }

    private sealed class ValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ValidationMessageBHandler : IMessageHandler<ValidationMessageB>
    {
        public Task HandleAsync(
            ValidationMessageB message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class RawInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult>(
                new InboundMessageInspectionResult(
                    RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator,
                    typeof(ValidationMessageA)
                )
                {
                    Message = new ValidationMessageA("raw")
                }
            );
        }
    }
}
