using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
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
    public async Task PublishMessageAsync_PublishesAcrossRabbitMqTopologiesEndToEnd()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddSingleton<RabbitMqIntegrationSerializer>();
            services.AddRabbitMqMessagePublishing(
                builder =>
                {
                    builder.UseConnectionFactory(
                        _ => new ConnectionFactory
                        {
                            Uri = new Uri(container.GetConnectionString())
                        }
                    );

                    builder.Exchange("orders-direct", ExchangeType.Direct);
                    builder.Exchange("orders-topic", ExchangeType.Topic);
                    builder.Exchange("orders-fanout", ExchangeType.Fanout);
                    builder.Exchange("orders-headers", ExchangeType.Headers);
                    builder.Exchange("orders-upstream", ExchangeType.Direct);
                    builder.Exchange("orders-downstream", ExchangeType.Direct);

                    builder.Queue("orders-direct-queue");
                    builder.Queue("orders-topic-queue");
                    builder.Queue("orders-fanout-queue");
                    builder.Queue("orders-headers-queue");
                    builder.Queue("orders-exchange-binding-queue");

                    builder.QueueBinding("orders-direct", "orders-direct-queue", "orders.created");
                    builder.QueueBinding("orders-topic", "orders-topic-queue", "orders.topic");
                    builder.QueueBinding("orders-fanout", "orders-fanout-queue");
                    builder.QueueBinding(
                        "orders-headers",
                        "orders-headers-queue",
                        configure: binding => binding
                           .WithArgument("x-match", "all")
                           .WithArgument("tenant", "tenant-headers")
                           .WithArgument("region", "us")
                    );
                    builder.ExchangeBinding("orders-upstream", "orders-downstream", "orders.exchange");
                    builder.QueueBinding("orders-downstream", "orders-exchange-binding-queue", "orders.exchange");

                    builder.Publish<RabbitMqPublishMessage>(
                        route => route
                           .ToDirectExchange("orders-direct", "orders.created")
                           .WithSerializer<RabbitMqIntegrationSerializer>()
                    );
                    builder.PublishNamed<RabbitMqPublishMessage>(
                        "topic-target",
                        route => route
                           .ToTopicExchange("orders-topic", static message => $"orders.{message.Name}")
                           .WithSerializer<RabbitMqIntegrationSerializer>()
                    );
                    builder.PublishNamed<RabbitMqPublishMessage>(
                        "fanout-target",
                        route => route
                           .ToFanoutExchange("orders-fanout")
                           .WithSerializer<RabbitMqIntegrationSerializer>()
                    );
                    builder.PublishNamed<RabbitMqPublishMessage>(
                        "headers-target",
                        route => route
                           .ToHeadersExchange("orders-headers")
                           .WithHeader("tenant", "route-tenant")
                           .WithHeader("region", "us")
                           .WithSerializer<RabbitMqIntegrationSerializer>()
                    );
                    builder.PublishNamed<RabbitMqPublishMessage>(
                        "exchange-binding-target",
                        route => route
                           .ToDirectExchange("orders-upstream", "orders.exchange")
                           .WithSerializer<RabbitMqIntegrationSerializer>()
                    );
                }
            );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            var targetRegistry = serviceProvider.GetRequiredService<ITargetRegistry>();

            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(42, "created"),
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(43, "topic"),
                targetRegistry.GetRequiredTarget("topic-target"),
                cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(44, "fanout"),
                targetRegistry.GetRequiredTarget("fanout-target"),
                cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(45, "headers"),
                targetRegistry.GetRequiredTarget("headers-target"),
                cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(46, "exchange"),
                targetRegistry.GetRequiredTarget("exchange-binding-target"),
                cancellationToken
            );

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var directMessage = await GetRequiredMessageAsync(channel, "orders-direct-queue", cancellationToken);
            var topicMessage = await GetRequiredMessageAsync(channel, "orders-topic-queue", cancellationToken);
            var fanoutMessage = await GetRequiredMessageAsync(channel, "orders-fanout-queue", cancellationToken);
            var headersMessage = await GetRequiredMessageAsync(channel, "orders-headers-queue", cancellationToken);
            var exchangeBindingMessage = await GetRequiredMessageAsync(
                channel,
                "orders-exchange-binding-queue",
                cancellationToken
            );

            Encoding.UTF8.GetString(directMessage.Body.ToArray()).Should().Be("{\"Id\":42,\"Name\":\"created\"}");
            Encoding.UTF8.GetString(topicMessage.Body.ToArray()).Should().Be("{\"Id\":43,\"Name\":\"topic\"}");
            Encoding.UTF8.GetString(fanoutMessage.Body.ToArray()).Should().Be("{\"Id\":44,\"Name\":\"fanout\"}");
            Encoding.UTF8.GetString(headersMessage.Body.ToArray()).Should().Be("{\"Id\":45,\"Name\":\"headers\"}");
            Encoding.UTF8.GetString(exchangeBindingMessage.Body.ToArray()).Should()
               .Be("{\"Id\":46,\"Name\":\"exchange\"}");

            headersMessage.BasicProperties.Should().NotBeNull();
            headersMessage.BasicProperties!.Headers.Should().NotBeNull();
            ExtractHeaderValue(headersMessage.BasicProperties.Headers!, "tenant").Should().Be("tenant-headers");
            ExtractHeaderValue(headersMessage.BasicProperties.Headers!, "region").Should().Be("us");

            serviceProvider.GetRequiredService<IMessageTopology>().GetRequiredTarget<RabbitMqPublishMessage>().Name
               .Should().Be(typeof(RabbitMqPublishMessage).FullName);
            targetRegistry.GetRequiredTarget("topic-target").Should().NotBeNull();
            targetRegistry.GetRequiredTarget("fanout-target").Should().NotBeNull();
            targetRegistry.GetRequiredTarget("headers-target").Should().NotBeNull();
            targetRegistry.GetRequiredTarget("exchange-binding-target").Should().NotBeNull();
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    private static async Task<BasicGetResult> GetRequiredMessageAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken
    )
    {
        for (var attempt = 0; attempt < 40; attempt++)
        {
            var result = await channel.BasicGetAsync(queueName, true, cancellationToken);

            if (result is not null)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        throw new InvalidOperationException($"No RabbitMQ message was available in queue '{queueName}'.");
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
