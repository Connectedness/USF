using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Integration;

public sealed class RabbitMqPublishingIntegrationTests
{
    [Fact]
    public async Task PublishMessageAsync_PublishesToRabbitMqEndToEnd()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddSingleton<RabbitMqIntegrationSerializer>();
            services.AddSingleton<IMessageSerializer>(
                static serviceProvider => serviceProvider.GetRequiredService<RabbitMqIntegrationSerializer>()
            );
            services.AddRabbitMqMessagePublishing(
                builder =>
                {
                    builder.UseConnectionFactory(
                        _ => new ConnectionFactory
                        {
                            Uri = new Uri(container.GetConnectionString())
                        }
                    );
                    builder.Exchange("orders-exchange", ExchangeType.Direct);
                    builder.Queue("orders-queue");
                    builder.Binding("orders-exchange", "orders-queue", "orders.created");
                    builder.Publish<RabbitMqPublishMessage>(
                        route => route
                           .ToExchange("orders-exchange", "orders.created")
                           .WithSerializer<RabbitMqIntegrationSerializer>()
                           .WithTargetName("orders-target")
                    );
                }
            );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            var message = new RabbitMqPublishMessage(42, "alpha");

            await publisher.PublishMessageAsync(message, cancellationToken: cancellationToken);

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            var result = await channel.BasicGetAsync("orders-queue", true, cancellationToken);

            result.Should().NotBeNull();
            Encoding.UTF8.GetString(result!.Body.ToArray()).Should().Be("{\"Id\":42,\"Name\":\"alpha\"}");
            result.BasicProperties.Should().NotBeNull();
            result.BasicProperties!.CorrelationId.Should().Be("corr-42");
            result.BasicProperties.Headers.Should().NotBeNull();
            ExtractHeaderValue(result.BasicProperties.Headers!, "tenant").Should().Be("tenant-alpha");
            serviceProvider.GetRequiredService<ITargetRegistry>().GetRequiredTarget("orders-target").Should()
               .NotBeNull();
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    private static string ExtractHeaderValue(IDictionary<string, object?> headers, string name)
    {
        var value = headers[name];

        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string text => text,
            _ => value?.ToString() ?? string.Empty
        };
    }
}
