using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Integration;

public sealed class RabbitMqDedicatedTopologiesIntegrationTests
{
    [Fact]
    public async Task DedicatedOutboundAndInboundTopologies_PublishAndConsumeAcrossTwoConnections()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13.7-management").Build();
        await container.StartAsync(cancellationToken);

        try
        {
            ConsumedMessageSink sink = new ();
            var services = new ServiceCollection();
            services.AddSingleton(sink);
            services
               .AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    outbound => outbound
                       .UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        )
                       .Exchange("inbound-events", ExchangeType.Direct)
                       .Address("inbound-events-address", "inbound-events")
                       .Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToDirectAddress("inbound-events-address", "published")
                               .WithSerializer<CloudEventMessageSerializer>()
                        )
                )
               .AddRabbitMqInboundTopology(
                    inbound => inbound
                       .UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
                            }
                        )
                       .Exchange("inbound-events", ExchangeType.Direct)
                       .Queue("inbound-events-queue")
                       .QueueBinding("inbound-events", "inbound-events-queue", "published")
                       .Consume(
                            "inbound-events-queue",
                            endpoint => endpoint
                               .PrefetchCount(1)
                               .Concurrency(1)
                               .Handle<RabbitMqPublishMessage, RecordingPublishMessageHandler>()
                        )
                );

            await using var serviceProvider = services.BuildServiceProvider();
            var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();

            try
            {
                foreach (var hostedService in hostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }

                var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();

                await publisher.PublishMessageAsync(
                    new RabbitMqPublishMessage(42, "consumed"),
                    cancellationToken: cancellationToken
                );

                var consumed = await sink.WaitAsync(cancellationToken);

                consumed.Id.Should().Be(42);
                consumed.Name.Should().Be("consumed");

                var outboundTopology =
                    serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(Topology.DefaultName);
                var inboundTopology =
                    serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(RabbitMqTopology.DefaultInboundName);
                var outboundConnection = await outboundTopology.GetConnectionAsync(cancellationToken);
                var inboundConnection = await inboundTopology.GetConnectionAsync(cancellationToken);

                outboundConnection.Should().NotBeSameAs(inboundConnection);
            }
            finally
            {
                foreach (var hostedService in hostedServices.Reverse())
                {
                    await hostedService.StopAsync(CancellationToken.None);
                }
            }
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    private sealed class RecordingPublishMessageHandler : IMessageHandler<RabbitMqPublishMessage>
    {
        private readonly ConsumedMessageSink _sink;

        public RecordingPublishMessageHandler(ConsumedMessageSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            RabbitMqPublishMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ConsumedMessageSink
    {
        private readonly TaskCompletionSource<RabbitMqPublishMessage> _completion =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(RabbitMqPublishMessage message)
        {
            _completion.TrySetResult(message);
        }

        public async Task<RabbitMqPublishMessage> WaitAsync(CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<RabbitMqPublishMessage>) state!).TrySetCanceled(),
                _completion
            );
            return await _completion.Task.ConfigureAwait(false);
        }
    }
}
