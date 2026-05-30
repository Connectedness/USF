using System;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Configuration;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class AddRabbitMqOutboundTopologyTests
{
    [Fact]
    public void AddRabbitMqOutboundTopology_ReportsDeterministicValidationErrors()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqOutboundTopology(
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
        Action action = () => _ = serviceProvider.GetRequiredService<IOutboundTopology>();

        var exception = action.Should().Throw<OutboundTopologyValidationException>().Which;
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
    public void AddRabbitMqOutboundTopology_CompilesDistinctTargetTypesForRabbitMqRoutes()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqOutboundTopology(
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
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "topic-target",
                    target => target
                       .ToTopicAddress("topic-address", static message => $"topic.{message.Value}")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "fanout-target",
                    target => target
                       .ToFanoutAddress("fanout-address")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "headers-target",
                    target => target
                       .ToHeadersAddress("headers-address")
                       .WithHeader("region", "eu")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        var outboundTopology = serviceProvider.GetRequiredService<IOutboundTopology>();
        var targetRegistry = serviceProvider.GetRequiredService<IOutboundTargetRegistry>();

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
}
