using System;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
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
                builder.Queue("queue-a", queue => queue.WithDeclareMode(RabbitMqDeclareMode.None));
                builder.Binding(
                    "exchange-a",
                    "queue-a",
                    "route-a",
                    binding => binding.WithDeclareMode(RabbitMqDeclareMode.Active)
                );
                builder.Binding("missing-exchange", "missing-queue", "route-b");
                builder.Publish<ValidationMessageA>(
                    route => route.ToExchange("missing-exchange").WithTargetName("duplicate-target")
                );
                builder.Publish<ValidationMessageA>(
                    route => route.ToExchange("missing-exchange").WithTargetName("duplicate-target")
                );
                builder.Publish<ValidationMessageB>(
                    route => route.ToExchange("missing-exchange").WithTargetName("duplicate-target")
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<IMessageTopology>();

        var exception = action.Should().Throw<MessageTopologyValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "A RabbitMQ connection factory must be configured.",
            "Binding from exchange 'exchange-a' to queue 'queue-a' cannot use declare mode 'Active' when either referenced entity uses 'None'.",
            "Binding from exchange 'missing-exchange' to queue 'missing-queue' references an unknown exchange.",
            "Duplicate exchange 'exchange-a' is configured.",
            "Duplicate message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA, Usf.Transport.RabbitMq.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null' is configured.",
            "Duplicate target 'duplicate-target' is configured.",
            "Message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'.",
            "Message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'.",
            "Message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' must configure a serializer.",
            "Message route 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' references unknown exchange 'missing-exchange'."
        );
    }
}
