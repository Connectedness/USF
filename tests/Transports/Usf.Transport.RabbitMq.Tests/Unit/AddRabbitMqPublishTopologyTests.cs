using System;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Configuration;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class AddRabbitMqPublishTopologyTests
{
    [Fact]
    public void AddUsf_WiresRegistryPayloadCodecAndSerializer()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<ValidationMessageA>("tests.validation-a"));
        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<IPayloadCodec>().Should().BeOfType<Utf8JsonPayloadCodec>();
        serviceProvider.GetRequiredService<IMessageSerializer>().Should().BeOfType<CloudEventMessageSerializer>();
        serviceProvider
           .GetRequiredService<IMessageContractRegistry>()
           .GetDiscriminator(typeof(ValidationMessageA))
           .Should().Be("tests.validation-a");
    }

    [Fact]
    public void AddUsf_ValidatesSourceWhenOptionsAreResolved()
    {
        var services = new ServiceCollection();
        services.AddUsf().UseCloudEvents(options => options.Source = "   ");
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<IOptions<CloudEventsOptions>>().Value;

        act.Should().Throw<OptionsValidationException>();
    }

    [Fact]
    public void Compile_RejectsUnregisteredTypedTargetWhenTopologyIsCompiled()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Address("orders-address", "orders");
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutAddress("orders-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action action = () => _ = serviceProvider.GetRequiredService<ITopology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void Compile_AggregatesStructuralAndMessageContractErrorsIntoSingleException()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutAddress("missing-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action action = () => _ = serviceProvider.GetRequiredService<ITopology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown address 'missing-address'."
        );
        exception.ValidationErrors.Should().Contain(
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_ReportsDeterministicValidationErrors()
    {
        var services = new ServiceCollection();
        services.AddUsf().AddRabbitMqTopology(
            builder =>
            {
                builder.Exchange("exchange-a", ExchangeType.Direct);
                builder.Exchange("exchange-a", ExchangeType.Direct);
                builder.Exchange("internal-a", "internal");
                builder.Queue("queue-a");
                builder.Address("address-a", "exchange-a");
                builder.Address("address-a", "exchange-a");
                builder.Address("missing-address-exchange", "missing-address-exchange");
                builder.ChannelGroup("shared", 2);
                builder.ChannelGroup("shared", 2);
                builder.QueueBinding(
                    "missing-exchange",
                    "missing-queue",
                    "route-a",
                    binding => binding.WithBindingMode((RabbitMqBindingMode) 99)
                );
                builder.ExchangeBinding("exchange-a", "missing-destination", "route-b");
                builder.Publish<ValidationMessageA>(target => target.ToDirectAddress("missing-address", "route-a"));
                builder.Publish<ValidationMessageA>(target => target.ToFanoutAddress("address-a"));
                builder.PublishNamed<ValidationMessageA>(
                    "duplicate-target",
                    target => target.ToHeadersAddress("address-a").UseChannelGroup("missing-group")
                );
                builder.PublishNamed<ValidationMessageB>(
                    "duplicate-target",
                    target => target.ToTopicAddress("address-a", static message => message.Value)
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action action = () => _ = serviceProvider.GetRequiredService<ITopology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "A RabbitMQ connection factory must be configured.",
            "Address 'missing-address-exchange' references unknown exchange 'missing-address-exchange'.",
            "Channel group 'shared' is configured but no outbound target references it.",
            "Duplicate address 'address-a' is configured.",
            "Duplicate channel group 'shared' is configured.",
            "Duplicate exchange 'exchange-a' is configured.",
            "Duplicate target 'duplicate-target' is configured.",
            "Exchange 'internal-a' uses unsupported exchange type 'internal'.",
            "Exchange binding from exchange 'exchange-a' to exchange 'missing-destination' references unknown destination exchange 'missing-destination'.",
            "Message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' configures multiple default RabbitMQ outbound targets.",
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'duplicate-target' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'duplicate-target' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' references unknown channel group 'missing-group'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'headers'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown address 'missing-address'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' targets exchange 'exchange-a' of type 'direct', but requires 'fanout'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'topic'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown queue 'missing-queue'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown source exchange 'missing-exchange'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' uses unsupported binding mode '99'."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_RejectsDuplicateTopologyNames()
    {
        var services = new ServiceCollection();
        var builder = services.AddUsf();
        builder.AddRabbitMqTopology("shared", static _ => { });

        var action = () => builder.AddRabbitMqTopology("shared", static _ => { });

        action.Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'shared' is already registered. Registered topologies: shared.");
    }

    [Fact]
    public void AddRabbitMqTopology_CompilesDistinctTargetTypesForRabbitMqRoutes()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("direct-exchange", ExchangeType.Direct);
                    builder.Exchange("topic-exchange", ExchangeType.Topic);
                    builder.Exchange("fanout-exchange", ExchangeType.Fanout);
                    builder.Exchange("headers-exchange", ExchangeType.Headers);
                    builder.Address("direct-address", "direct-exchange");
                    builder.Address("topic-address", "topic-exchange");
                    builder.Address("fanout-address", "fanout-exchange");
                    builder.Address("headers-address", "headers-exchange");
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToDirectAddress("direct-address", "direct.route")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "topic-target",
                        target => target
                           .ToTopicAddress("topic-address", static message => $"topic.{message.Value}")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "fanout-target",
                        target => target
                           .ToFanoutAddress("fanout-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "headers-target",
                        target => target
                           .ToHeadersAddress("headers-address")
                           .WithHeader("region", "eu")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var outboundTopology = serviceProvider.GetRequiredService<ITopology>();
        var targetRegistry = serviceProvider.GetRequiredService<ITopology>();

        outboundTopology
           .GetRequiredTarget<ValidationMessageA>().GetType()
           .Name.Should().Be("RabbitMqDirectOutboundTarget`1");
        targetRegistry
           .GetRequiredTarget("topic-target").GetType()
           .Name.Should().Be("RabbitMqTopicOutboundTarget`1");
        targetRegistry
           .GetRequiredTarget("fanout-target").GetType()
           .Name.Should().Be("RabbitMqFanoutOutboundTarget`1");
        targetRegistry
           .GetRequiredTarget("headers-target").GetType()
           .Name.Should().Be("RabbitMqHeadersOutboundTarget`1");
    }

    [Fact]
    public void OutboundTopologyRegistry_ResolvesNamedTopologyTargets()
    {
        TopologyName topologyName = new ("named");
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                topologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Address("orders-address", "orders");
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutAddress("orders-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .GetRequiredTopology(topologyName);

        topology.GetRequiredTarget<ValidationMessageA>().TopologyName.Should().Be(topologyName);
    }

    [Fact]
    public void Compile_RejectsDialectEntryForMessageTypeThatNoTargetPublishes()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.MapMessageContracts(
                        contracts => contracts.MapOutbound<ValidationMessageB>("tests.unused-dialect")
                    );
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Address("orders-address", "orders");
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutAddress("orders-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<ITopology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "RabbitMQ outbound message-contract dialect maps message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB', but no outbound target publishes that type on this topology."
        );
    }

    [Fact]
    public void Compile_AllowsDialectOnlyMessageTypeWhenTargetPublishesIt()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
           .UseCloudEvents(options => options.Source = "/tests/rabbitmq")
           .MapMessageContracts(contracts => contracts.Map<ValidationMessageA>("tests.validation-a"))
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.MapMessageContracts(
                        contracts => contracts.MapOutbound<ValidationMessageB>("tests.dialect-only")
                    );
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Address("orders-address", "orders");
                    builder.Publish<ValidationMessageB>(
                        target => target
                           .ToFanoutAddress("orders-address")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var target = serviceProvider
           .GetRequiredService<ITopology>()
           .GetRequiredTarget<ValidationMessageB>();

        target.GetRequiredDiscriminator(typeof(ValidationMessageB)).Should().Be("tests.dialect-only");
    }

    [Fact]
    public void AddRabbitMqTopology_RejectsMandatoryTargetsUsingFireAndForgetPublishing()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.WithDefaultPublisherConfirmMode(RabbitMqPublisherConfirmMode.FireAndForget);
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Address("orders-address", "orders");
                    builder.ChannelGroup("best-effort", 1, RabbitMqPublisherConfirmMode.FireAndForget);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutAddress("orders-address")
                           .Mandatory()
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "shared-best-effort",
                        target => target
                           .ToFanoutAddress("orders-address")
                           .UseChannelGroup("best-effort")
                           .Mandatory()
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action action = () => _ = serviceProvider.GetRequiredService<ITopology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().BeEquivalentTo(
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' enables mandatory routing but its effective channel group uses fire-and-forget publishing.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'shared-best-effort' enables mandatory routing but its effective channel group uses fire-and-forget publishing."
        );
    }
}
