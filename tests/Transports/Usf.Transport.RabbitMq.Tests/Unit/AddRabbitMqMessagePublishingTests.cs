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

public sealed class AddRabbitMqMessagePublishingTests
{
    [Fact]
    public void AddRabbitMqMessagePublishing_ReportsDeterministicValidationErrors()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqMessagePublishing(
            builder =>
            {
                builder.Exchange("exchange-a", ExchangeType.Direct);
                builder.Exchange("exchange-a", ExchangeType.Direct);
                builder.Exchange("fanout-a", ExchangeType.Fanout);
                builder.Exchange("internal-a", "internal");
                builder.Queue("queue-a");
                builder.QueueBinding(
                    "missing-exchange",
                    "missing-queue",
                    "route-a",
                    binding => binding.WithDeclareMode((RabbitMqBindingDeclareMode) 99)
                );
                builder.ExchangeBinding("exchange-a", "missing-destination", "route-b");
                builder.Publish<ValidationMessageA>(route => route.ToDirectExchange("missing-exchange", "route-a"));
                builder.Publish<ValidationMessageA>(route => route.ToFanoutExchange("fanout-a"));
                builder.PublishNamed<ValidationMessageA>(
                    "duplicate-target",
                    route => route.ToHeadersExchange("exchange-a")
                );
                builder.PublishNamed<ValidationMessageB>(
                    "duplicate-target",
                    route => route.ToTopicExchange("exchange-a", static message => message.Value)
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action action = () => _ = serviceProvider.GetRequiredService<IMessageTopology>();

        var exception = action.Should().Throw<MessageTopologyValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "A RabbitMQ connection factory must be configured.",
            "Duplicate exchange 'exchange-a' is configured.",
            "Duplicate target 'duplicate-target' is configured.",
            "Exchange 'internal-a' uses unsupported exchange type 'internal'.",
            "Exchange binding from exchange 'exchange-a' to exchange 'missing-destination' references unknown destination exchange 'missing-destination'.",
            "Message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' configures multiple default RabbitMQ publish routes.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' must configure a serializer.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'headers'.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' must configure a serializer.",
            "Publish route for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'topic'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown queue 'missing-queue'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown source exchange 'missing-exchange'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' uses unsupported declare mode '99'."
        );
    }

    [Fact]
    public void AddRabbitMqMessagePublishing_CompilesDistinctTargetTypesForRabbitMqRoutes()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqMessagePublishing(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.Exchange("direct-exchange", ExchangeType.Direct);
                builder.Exchange("topic-exchange", ExchangeType.Topic);
                builder.Exchange("fanout-exchange", ExchangeType.Fanout);
                builder.Exchange("headers-exchange", ExchangeType.Headers);
                builder.Publish<ValidationMessageA>(
                    route => route
                       .ToDirectExchange("direct-exchange", "direct.route")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "topic-target",
                    route => route
                       .ToTopicExchange("topic-exchange", static message => $"topic.{message.Value}")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "fanout-target",
                    route => route
                       .ToFanoutExchange("fanout-exchange")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
                builder.PublishNamed<ValidationMessageA>(
                    "headers-target",
                    route => route
                       .ToHeadersExchange("headers-exchange")
                       .WithHeader("region", "eu")
                       .WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        var messageTopology = serviceProvider.GetRequiredService<IMessageTopology>();
        var targetRegistry = serviceProvider.GetRequiredService<ITargetRegistry>();

        messageTopology.GetRequiredTarget<ValidationMessageA>().GetType().Name.Should().Be("RabbitMqDirectTarget`1");
        targetRegistry.GetRequiredTarget("topic-target").GetType().Name.Should().Be("RabbitMqTopicTarget`1");
        targetRegistry.GetRequiredTarget("fanout-target").GetType().Name.Should().Be("RabbitMqFanoutTarget`1");
        targetRegistry.GetRequiredTarget("headers-target").GetType().Name.Should().Be("RabbitMqHeadersTarget`1");
    }
}
