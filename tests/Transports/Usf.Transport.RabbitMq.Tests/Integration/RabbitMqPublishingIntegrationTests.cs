using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Testcontainers.RabbitMq;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Configuration;
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
            await DeclareExchangeAsync(
                container.GetConnectionString(),
                "orders-fanout",
                ExchangeType.Fanout,
                cancellationToken
            );

            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
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
                        builder.Exchange(
                            "orders-fanout",
                            ExchangeType.Fanout,
                            exchange => exchange.WithDeclareMode(RabbitMqDeclareMode.Passive)
                        );
                        builder.Exchange("orders-headers", ExchangeType.Headers);
                        builder.Exchange("orders-upstream", ExchangeType.Direct);
                        builder.Exchange("orders-downstream", ExchangeType.Direct);
                        builder.Address("orders-direct-address", "orders-direct");
                        builder.Address("orders-topic-address", "orders-topic");
                        builder.Address("orders-fanout-address", "orders-fanout");
                        builder.Address("orders-headers-address", "orders-headers");
                        builder.Address("orders-upstream-address", "orders-upstream");

                        builder.Queue("orders-direct-queue");
                        builder.Queue("orders-audit-queue");
                        builder.Queue("orders-topic-queue");
                        builder.Queue("orders-fanout-queue");
                        builder.Queue("orders-headers-queue");
                        builder.Queue("orders-exchange-binding-queue");

                        builder.QueueBinding("orders-direct", "orders-direct-queue", "orders.created");
                        builder.QueueBinding("orders-direct", "orders-audit-queue", "orders.audit");
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
                               .ToDirectAddress("orders-direct-address", "orders.created")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.Publish<RabbitMqAuditMessage>(
                            route => route
                               .ToDirectAddress("orders-direct-address", "orders.audit")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.PublishNamed<RabbitMqPublishMessage>(
                            "topic-target",
                            route => route
                               .ToTopicAddress("orders-topic-address", static message => $"orders.{message.Name}")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.PublishNamed<RabbitMqPublishMessage>(
                            "fanout-target",
                            route => route
                               .ToFanoutAddress("orders-fanout-address")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.PublishNamed<RabbitMqPublishMessage>(
                            "headers-target",
                            route => route
                               .ToHeadersAddress("orders-headers-address")
                               .WithHeader("tenant", "tenant-headers")
                               .WithHeader("region", "us")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.PublishNamed<RabbitMqPublishMessage>(
                            "exchange-binding-target",
                            route => route
                               .ToDirectAddress("orders-upstream-address", "orders.exchange")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            var targetRegistry = serviceProvider.GetRequiredService<IOutboundTargetRegistry>();
            Activity? directProducerActivity = null;
            var directParentTraceId = default(ActivityTraceId);
            using var listener = new ActivityListener();
            listener.ShouldListenTo = source => source.Name == OutboundDiagnostics.ActivitySourceName;
            listener.Sample = static (ref _) => ActivitySamplingResult.AllData;
            listener.ActivityStarted = activity =>
            {
                // ReSharper disable once AccessToModifiedClosure -- OK in test scenario
                if (activity.OperationName == "usf.outbound.publish" && activity.TraceId == directParentTraceId)
                {
                    directProducerActivity = activity;
                }
            };
            ActivitySource.AddActivityListener(listener);

            var directId = Guid.Parse("7e8ee037-b02f-4c6f-a0ed-9530a342da45");
            var directTime = new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero);
            CloudEventMetadata directMetadata = new (directId, directTime, "order-42");
            using (var directParentActivity = new Activity("direct-parent").SetIdFormat(ActivityIdFormat.W3C).Start())
            {
                directParentTraceId = directParentActivity.TraceId;
                await publisher.PublishMessageAsync(
                    new RabbitMqPublishMessage(42, "created"),
                    in directMetadata,
                    cancellationToken: cancellationToken
                );
            }

            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(43, "topic"),
                targetRegistry.GetRequiredTarget("topic-target"),
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqAuditMessage(1042, "audit"),
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(44, "fanout"),
                targetRegistry.GetRequiredTarget("fanout-target"),
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(45, "headers"),
                targetRegistry.GetRequiredTarget("headers-target"),
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(46, "exchange"),
                targetRegistry.GetRequiredTarget("exchange-binding-target"),
                cancellationToken: cancellationToken
            );

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var directMessage = await GetRequiredMessageAsync(channel, "orders-direct-queue", cancellationToken);
            var auditMessage = await GetRequiredMessageAsync(channel, "orders-audit-queue", cancellationToken);
            var topicMessage = await GetRequiredMessageAsync(channel, "orders-topic-queue", cancellationToken);
            var fanoutMessage = await GetRequiredMessageAsync(channel, "orders-fanout-queue", cancellationToken);
            var headersMessage = await GetRequiredMessageAsync(channel, "orders-headers-queue", cancellationToken);
            var exchangeBindingMessage = await GetRequiredMessageAsync(
                channel,
                "orders-exchange-binding-queue",
                cancellationToken
            );

            Encoding.UTF8.GetString(directMessage.Body.ToArray()).Should().Be("{\"Id\":42,\"Name\":\"created\"}");
            Encoding.UTF8.GetString(auditMessage.Body.ToArray()).Should().Be("{\"Id\":1042,\"EventName\":\"audit\"}");
            Encoding.UTF8.GetString(topicMessage.Body.ToArray()).Should().Be("{\"Id\":43,\"Name\":\"topic\"}");
            Encoding.UTF8.GetString(fanoutMessage.Body.ToArray()).Should().Be("{\"Id\":44,\"Name\":\"fanout\"}");
            Encoding.UTF8.GetString(headersMessage.Body.ToArray()).Should().Be("{\"Id\":45,\"Name\":\"headers\"}");
            Encoding.UTF8.GetString(exchangeBindingMessage.Body.ToArray())
               .Should().Be("{\"Id\":46,\"Name\":\"exchange\"}");

            directMessage.BasicProperties.Should().NotBeNull();
            directMessage.BasicProperties.ContentType.Should().Be("application/json");
            directMessage.BasicProperties.MessageId.Should().Be(directId.ToString("D"));
            directMessage.BasicProperties.Headers.Should().NotBeNull();
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:id")
               .Should().Be(directId.ToString("D"));
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:specversion").Should().Be("1.0");
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:source")
               .Should().Be("/tests/rabbitmq");
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:type")
               .Should().Be(RabbitMqCloudEventsTestFactory.PublishMessageDiscriminator);
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:time")
               .Should().Be(directTime.ToString("O"));
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:subject")
               .Should().Be("order-42");
            ExtractHeaderValue(directMessage.BasicProperties.Headers!, "cloudEvents:dataschema")
               .Should().Be("/schemas/rabbitmq-publish");
            directProducerActivity.Should().NotBeNull();
            var traceParent = ExtractHeaderValue(directMessage.BasicProperties.Headers!, "traceparent");
            ActivityContext.TryParse(traceParent, traceState: null, out var traceContext).Should().BeTrue();
            traceContext.TraceId.Should().Be(directProducerActivity!.TraceId);
            traceContext.SpanId.Should().Be(directProducerActivity.SpanId);
            directMessage.BasicProperties.Headers.Should().NotContainKey("cloudEvents:traceparent");

            headersMessage.BasicProperties.Should().NotBeNull();
            headersMessage.BasicProperties.Headers.Should().NotBeNull();
            ExtractHeaderValue(headersMessage.BasicProperties.Headers!, "tenant").Should().Be("tenant-headers");
            ExtractHeaderValue(headersMessage.BasicProperties.Headers!, "region").Should().Be("us");

            serviceProvider
               .GetRequiredService<IOutboundTopology>()
               .GetRequiredTarget<RabbitMqPublishMessage>().Name
               .Should().Be(typeof(RabbitMqPublishMessage).FullName);
            serviceProvider
               .GetRequiredService<IOutboundTopology>()
               .GetRequiredTarget<RabbitMqAuditMessage>().Name
               .Should().Be(typeof(RabbitMqAuditMessage).FullName);
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

    [Fact]
    public async Task PublishMessageAsync_PublishesSameClrTypeWithPerTopologyContracts()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            TopologyName legacy = new ("legacy");
            TopologyName modern = new ("modern");
            var services = new ServiceCollection();
            services
               .AddUsf()
               .UseCloudEvents(options => options.Source = "/tests/rabbitmq")
               .MapMessageContracts(
                    contracts => contracts.Map<RabbitMqPublishMessage>("tests.rabbitmq.publish.canonical")
                       .WithDataSchema("/schemas/canonical")
                )
               .AddRabbitMqOutboundTopology(
                    legacy,
                    builder =>
                    {
                        builder.MapMessageContracts(
                            contracts => contracts.MapOutbound<RabbitMqPublishMessage>("tests.rabbitmq.publish.legacy")
                               .WithDataSchema("/schemas/legacy")
                        );
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );
                        builder.Exchange("legacy-fanout", ExchangeType.Fanout);
                        builder.Address("legacy-address", "legacy-fanout");
                        builder.Queue("legacy-queue");
                        builder.QueueBinding("legacy-fanout", "legacy-queue");
                        builder.Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToFanoutAddress("legacy-address")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                )
               .AddRabbitMqOutboundTopology(
                    modern,
                    builder =>
                    {
                        builder.MapMessageContracts(
                            contracts => contracts.MapOutbound<RabbitMqPublishMessage>("tests.rabbitmq.publish.modern")
                               .WithDataSchema("/schemas/modern")
                        );
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );
                        builder.Exchange("modern-fanout", ExchangeType.Fanout);
                        builder.Address("modern-address", "modern-fanout");
                        builder.Queue("modern-queue");
                        builder.QueueBinding("modern-fanout", "modern-queue");
                        builder.Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToFanoutAddress("modern-address")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            var hostedService = serviceProvider.GetServices<IHostedService>().Should().ContainSingle().Which;
            await hostedService.StartAsync(cancellationToken);

            var legacyTopology = serviceProvider.GetRequiredKeyedService<RabbitMqOutboundTopology>(legacy);
            var modernTopology = serviceProvider.GetRequiredKeyedService<RabbitMqOutboundTopology>(modern);
            var legacyConnection = await legacyTopology.GetConnectionAsync(cancellationToken);
            var modernConnection = await modernTopology.GetConnectionAsync(cancellationToken);
            legacyConnection.Should().NotBeSameAs(modernConnection);

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            await publisher
               .ForTopology(legacy)
               .PublishMessageAsync(new RabbitMqPublishMessage(100, "legacy"), cancellationToken: cancellationToken);
            await publisher
               .ForTopology(modern)
               .PublishMessageAsync(new RabbitMqPublishMessage(200, "modern"), cancellationToken: cancellationToken);

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var legacyMessage = await GetRequiredMessageAsync(channel, "legacy-queue", cancellationToken);
            var modernMessage = await GetRequiredMessageAsync(channel, "modern-queue", cancellationToken);

            Encoding.UTF8.GetString(legacyMessage.Body.ToArray()).Should().Be("{\"Id\":100,\"Name\":\"legacy\"}");
            ExtractHeaderValue(legacyMessage.BasicProperties.Headers!, "cloudEvents:type")
               .Should().Be("tests.rabbitmq.publish.legacy");
            ExtractHeaderValue(legacyMessage.BasicProperties.Headers!, "cloudEvents:dataschema")
               .Should().Be("/schemas/legacy");
            Encoding.UTF8.GetString(modernMessage.Body.ToArray()).Should().Be("{\"Id\":200,\"Name\":\"modern\"}");
            ExtractHeaderValue(modernMessage.BasicProperties.Headers!, "cloudEvents:type")
               .Should().Be("tests.rabbitmq.publish.modern");
            ExtractHeaderValue(modernMessage.BasicProperties.Headers!, "cloudEvents:dataschema")
               .Should().Be("/schemas/modern");
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishMessageAsync_OverridesConstantDirectRoutingKeyWithCallerRoutingKey()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );

                        builder.Exchange("routing-direct", ExchangeType.Direct);
                        builder.Address("routing-direct-address", "routing-direct");
                        builder.Queue("routing-direct-default-queue");
                        builder.Queue("routing-direct-override-queue");
                        builder.QueueBinding("routing-direct", "routing-direct-default-queue", "orders.created");
                        builder.QueueBinding("routing-direct", "routing-direct-override-queue", "orders.override");

                        builder.Publish<RabbitMqPublishMessage>(
                            route => route
                               .ToDirectAddress("routing-direct-address", "orders.created")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();

            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(60, "created"),
                routingKey: "orders.override",
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(61, "created"),
                cancellationToken: cancellationToken
            );

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var overrideMessage = await GetRequiredMessageAsync(
                channel,
                "routing-direct-override-queue",
                cancellationToken
            );
            var defaultMessage = await GetRequiredMessageAsync(
                channel,
                "routing-direct-default-queue",
                cancellationToken
            );

            Encoding.UTF8.GetString(overrideMessage.Body.ToArray()).Should().Be("{\"Id\":60,\"Name\":\"created\"}");
            Encoding.UTF8.GetString(defaultMessage.Body.ToArray()).Should().Be("{\"Id\":61,\"Name\":\"created\"}");
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishMessageAsync_OverridesTopicRoutingKeyFactoryWithCallerRoutingKey()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );

                        builder.Exchange("routing-topic", ExchangeType.Topic);
                        builder.Address("routing-topic-address", "routing-topic");
                        builder.Queue("routing-topic-default-queue");
                        builder.Queue("routing-topic-override-queue");
                        builder.QueueBinding("routing-topic", "routing-topic-default-queue", "orders.topic");
                        builder.QueueBinding("routing-topic", "routing-topic-override-queue", "orders.override");

                        builder.Publish<RabbitMqPublishMessage>(
                            route => route
                               .ToTopicAddress("routing-topic-address", static message => $"orders.{message.Name}")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();

            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(70, "topic"),
                routingKey: "orders.override",
                cancellationToken: cancellationToken
            );
            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(71, "topic"),
                cancellationToken: cancellationToken
            );

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var overrideMessage = await GetRequiredMessageAsync(
                channel,
                "routing-topic-override-queue",
                cancellationToken
            );
            var defaultMessage = await GetRequiredMessageAsync(
                channel,
                "routing-topic-default-queue",
                cancellationToken
            );

            Encoding.UTF8.GetString(overrideMessage.Body.ToArray()).Should().Be("{\"Id\":70,\"Name\":\"topic\"}");
            Encoding.UTF8.GetString(defaultMessage.Body.ToArray()).Should().Be("{\"Id\":71,\"Name\":\"topic\"}");
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishRawAsync_PublishesCallerOwnedEnvelopeWithoutUsfSerialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );

                        builder.Exchange("raw-fanout", ExchangeType.Fanout);
                        builder.Address("raw-address", "raw-fanout");
                        builder.Queue("raw-queue");
                        builder.QueueBinding("raw-fanout", "raw-queue");

                        builder.Publish<RabbitMqPublishMessage>(
                            route => route
                               .ToFanoutAddress("raw-address")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            var target = serviceProvider
               .GetRequiredService<IOutboundTopology>()
               .GetRequiredTarget<RabbitMqPublishMessage>();

            // A payload that is deliberately not the JSON the serializer would produce proving USF
            // serialization is bypassed, and the envelope is owned entirely by the caller.
            SerializedMessage rawMessage = new (
                "raw-payload"u8.ToArray(),
                "application/custom",
                "utf-8",
                new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    ["tenant"] = "tenant-7"
                },
                "message-id-7",
                "correlation-id-7"
            );

            await publisher.PublishRawAsync(rawMessage, target, cancellationToken);

            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(container.GetConnectionString())
            };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            var received = await GetRequiredMessageAsync(channel, "raw-queue", cancellationToken);

            Encoding.UTF8.GetString(received.Body.ToArray()).Should().Be("raw-payload");
            received.BasicProperties.Should().NotBeNull();
            received.BasicProperties.ContentType.Should().Be("application/custom");
            received.BasicProperties.ContentEncoding.Should().Be("utf-8");
            received.BasicProperties.MessageId.Should().Be("message-id-7");
            received.BasicProperties.CorrelationId.Should().Be("correlation-id-7");
            received.BasicProperties.Headers.Should().NotBeNull();
            ExtractHeaderValue(received.BasicProperties.Headers!, "tenant").Should().Be("tenant-7");
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsReturnedDeliveryFailureForUnroutableMandatoryMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );

                        builder.Exchange("unroutable-fanout", ExchangeType.Fanout);
                        builder.Address("unroutable-address", "unroutable-fanout");
                        builder.Publish<RabbitMqPublishMessage>(
                            route => route
                               .ToFanoutAddress("unroutable-address")
                               .Mandatory()
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();

            var action = async () => await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(47, "unroutable"),
                cancellationToken: cancellationToken
            );

            var exception = (await action.Should().ThrowAsync<MessageDeliveryException>()).Which;
            exception
               .TargetName.Should()
               .Be(typeof(RabbitMqPublishMessage).FullName);
            exception.Reason.Should().Be(MessageDeliveryFailureReason.Returned);
            exception.InnerException.Should().BeAssignableTo<PublishException>();
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsNackedDeliveryFailureWhenBrokerRejectsPublish()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        );

                        builder.Exchange("rejecting-fanout", ExchangeType.Fanout);
                        builder.Address("rejecting-address", "rejecting-fanout");

                        // A length-bounded queue with reject-publish overflow makes the broker NACK any publish
                        // once the queue is full. With confirms on (the default), the first awaited publish fills
                        // the queue, so the second deterministically surfaces as a Nacked delivery failure.
                        builder.Queue(
                            "rejecting-queue",
                            queue => queue
                               .WithMaxLength(1)
                               .WithArgument("x-overflow", "reject-publish")
                        );
                        builder.QueueBinding("rejecting-fanout", "rejecting-queue");

                        builder.Publish<RabbitMqPublishMessage>(
                            route => route
                               .ToFanoutAddress("rejecting-address")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();

            await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(48, "accepted"),
                cancellationToken: cancellationToken
            );

            var action = async () => await publisher.PublishMessageAsync(
                new RabbitMqPublishMessage(49, "rejected"),
                cancellationToken: cancellationToken
            );

            var exception = (await action.Should().ThrowAsync<MessageDeliveryException>()).Which;
            exception
               .TargetName.Should()
               .Be(typeof(RabbitMqPublishMessage).FullName);
            exception.Reason.Should().Be(MessageDeliveryFailureReason.Nacked);
            exception.InnerException.Should().BeAssignableTo<PublishException>();
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishMessageAsync_RecoversAfterBrokerRestartWithoutExhaustingSingleChannelPool()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var hostPort = GetAvailableTcpPort();
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13-management")
           .WithPortBinding(hostPort, 5672)
           .Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var connectionString = container.GetConnectionString();
            var mappedPort = container.GetMappedPublicPort(5672);
            var services = new ServiceCollection();
            services.AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(connectionString),
                                NetworkRecoveryInterval = TimeSpan.FromMilliseconds(200)
                            }
                        );

                        builder.Exchange("recovering-fanout", ExchangeType.Fanout);
                        builder.Address("recovering-address", "recovering-fanout");
                        builder.Queue("recovering-queue");
                        builder.QueueBinding("recovering-fanout", "recovering-queue");
                        builder.ChannelGroup(
                            "recovering-group",
                            1,
                            RabbitMqPublisherConfirmMode.Confirms,
                            TimeSpan.FromSeconds(2)
                        );
                        builder.Publish<RabbitMqPublishMessage>(
                            route => route
                               .ToFanoutAddress("recovering-address")
                               .UseChannelGroup("recovering-group")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            var topology = serviceProvider.GetRequiredService<RabbitMqOutboundTopology>();
            var connection = await topology.GetConnectionAsync(cancellationToken);
            var recovered = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var recoveryFailures = new ConcurrentQueue<Exception>();
            AsyncEventHandler<ConnectionRecoveryErrorEventArgs> recoveryErrorHandler = (_, eventArgs) =>
            {
                recoveryFailures.Enqueue(eventArgs.Exception);
                return Task.CompletedTask;
            };
            AsyncEventHandler<AsyncEventArgs> recoveryHandler = (_, _) =>
            {
                recovered.TrySetResult(null);
                return Task.CompletedTask;
            };
            connection.ConnectionRecoveryErrorAsync += recoveryErrorHandler;
            connection.RecoverySucceededAsync += recoveryHandler;

            try
            {
                await publisher.PublishMessageAsync(
                    new RabbitMqPublishMessage(50, "before-restart"),
                    cancellationToken: cancellationToken
                );

                await container.StopAsync(cancellationToken);

                using var outageCancellationTokenSource =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                outageCancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(5));
                var stopwatch = Stopwatch.StartNew();
                var publishDuringOutage = async () => await publisher.PublishMessageAsync(
                    new RabbitMqPublishMessage(51, "during-outage"),
                    // ReSharper disable once AccessToDisposedClosure -- delegate is called before disposal
                    cancellationToken: outageCancellationTokenSource.Token
                );

                await publishDuringOutage.Should().ThrowAsync<Exception>();
                stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10));

                await container.StartAsync(cancellationToken);
                container.GetMappedPublicPort(5672).Should().Be(mappedPort);

                try
                {
                    await recovered.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
                }
                catch (TimeoutException exception)
                {
                    throw new TimeoutException(
                        $"RabbitMQ.Client did not recover the connection. IsOpen={connection.IsOpen}. Recovery failures: {string.Join(" | ", recoveryFailures)}",
                        exception
                    );
                }

                for (var id = 52; id < 62; id++)
                {
                    await publisher.PublishMessageAsync(
                        new RabbitMqPublishMessage(id, $"after-restart-{id}"),
                        cancellationToken: cancellationToken
                    );
                }
            }
            finally
            {
                connection.ConnectionRecoveryErrorAsync -= recoveryErrorHandler;
                connection.RecoverySucceededAsync -= recoveryHandler;
            }
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

    private static int GetAvailableTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        try
        {
            return ((IPEndPoint) listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task DeclareExchangeAsync(
        string connectionString,
        string exchangeName,
        string exchangeType,
        CancellationToken cancellationToken
    )
    {
        var connectionFactory = new ConnectionFactory
        {
            Uri = new Uri(connectionString)
        };
        await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        await channel.ExchangeDeclareAsync(
            exchangeName,
            exchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken
        );
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
