using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// The active consumer runtime for a RabbitMQ topology that contains inbound endpoints. It opens consumer
/// channels, starts <c>BasicConsume</c> for each endpoint, drains in-flight handlers on stop, and disposes the
/// topology's runtime resources. It is registered as an <see cref="ITopologyRuntime" /> only for topology
/// instances that contain inbound endpoints, and is started by the shared
/// <see cref="TopologyRuntimeHostedService" /> after topology provisioning completes.
/// </summary>
public sealed class RabbitMqTopologyRuntime : ITopologyRuntime
{
    private readonly List<IChannel> _channels = [];
    private readonly List<ConsumerRegistration> _consumerRegistrations = [];
    private readonly ConcurrentDictionary<long, InFlightDelivery> _inFlightDeliveries = new ();
    private readonly ILogger _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly RabbitMqTopology _topology;
    private long _nextInFlightId;
    private int _started;
    private CancellationTokenSource? _stoppingCancellationTokenSource;

    public RabbitMqTopologyRuntime(
        RabbitMqTopology topology,
        IServiceScopeFactory serviceScopeFactory,
        ILogger<RabbitMqTopologyRuntime>? logger = null
    )
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _serviceScopeFactory = serviceScopeFactory ?? throw new ArgumentNullException(nameof(serviceScopeFactory));
        _logger = logger ?? NullLogger<RabbitMqTopologyRuntime>.Instance;
    }

    public TopologyName TopologyName => _topology.Topology.TopologyName;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            return;
        }

        _stoppingCancellationTokenSource = new CancellationTokenSource();

        foreach (var endpointGroup in _topology.EndpointsByChannelGroup)
        {
            var channelGroup = endpointGroup.Key;
            var endpoints = endpointGroup.ToArray();

            for (var index = 0; index < channelGroup.MaximumChannelCount; index++)
            {
                var channel = await _topology.CreateChannelAsync(
                        channelGroup.CreateChannelOptions(),
                        cancellationToken
                    )
                   .ConfigureAwait(false);
                await channel.BasicQosAsync(
                        prefetchSize: 0,
                        prefetchCount: channelGroup.PrefetchCount,
                        global: false,
                        cancellationToken
                    )
                   .ConfigureAwait(false);
                _channels.Add(channel);

                foreach (var endpoint in endpoints)
                {
                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += (_, eventArgs) =>
                        OnReceivedAsync(endpoint, channel, eventArgs);
                    var consumerTag = await channel.BasicConsumeAsync(
                            queue: endpoint.QueueName,
                            autoAck: false,
                            consumerTag: string.Empty,
                            noLocal: false,
                            exclusive: false,
                            arguments: new Dictionary<string, object?>(0, StringComparer.Ordinal),
                            consumer: consumer,
                            cancellationToken: cancellationToken
                        )
                       .ConfigureAwait(false);
                    _consumerRegistrations.Add(new ConsumerRegistration(channel, consumerTag));
                }
            }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 0) == 0)
        {
            return;
        }

        _stoppingCancellationTokenSource?.Cancel();

        foreach (var registration in _consumerRegistrations)
        {
            try
            {
                await registration.Channel.BasicCancelAsync(
                        registration.ConsumerTag,
                        noWait: false,
                        cancellationToken
                    )
                   .ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ consumer cancel failed for consumer tag {ConsumerTag}",
                    registration.ConsumerTag
                );
            }
        }

        await DrainInFlightDeliveriesAsync(cancellationToken).ConfigureAwait(false);

        foreach (var channel in _channels)
        {
            if (channel is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                channel.Dispose();
            }
        }

        _channels.Clear();
        _consumerRegistrations.Clear();
        _stoppingCancellationTokenSource?.Dispose();
        _stoppingCancellationTokenSource = null;
        await _topology.DisposeAsync().ConfigureAwait(false);
    }

    private async Task DrainInFlightDeliveriesAsync(CancellationToken cancellationToken)
    {
        if (_inFlightDeliveries.IsEmpty)
        {
            return;
        }

        var inFlightTasks = _inFlightDeliveries.Values.Select(static delivery => delivery.Completion).ToArray();
        var drainTask = Task.WhenAll(inFlightTasks);
        var timeoutTask = Task.Delay(_topology.ShutdownTimeout, cancellationToken);

        if (await Task.WhenAny(drainTask, timeoutTask).ConfigureAwait(false) == drainTask)
        {
            await drainTask.ConfigureAwait(false);
            return;
        }

        foreach (var delivery in _inFlightDeliveries.Values)
        {
            await delivery.Acknowledgement.NackAsync(requeue: true, CancellationToken.None)
               .ConfigureAwait(false);
        }
    }

    private async Task OnReceivedAsync(
        RabbitMqInboundEndpoint subscribedEndpoint,
        IChannel channel,
        BasicDeliverEventArgs eventArgs
    )
    {
        var acknowledgement = new RabbitMqMessageAcknowledgement(channel, eventArgs.DeliveryTag);
        var inFlightId = Interlocked.Increment(ref _nextInFlightId);
        var inFlight = new InFlightDelivery(acknowledgement);
        _inFlightDeliveries[inFlightId] = inFlight;

        try
        {
            await ProcessDeliveryAsync(subscribedEndpoint, acknowledgement, eventArgs)
               .ConfigureAwait(false);
        }
        finally
        {
            _inFlightDeliveries.TryRemove(inFlightId, out _);
            inFlight.SetCompleted();
        }
    }

    private async Task ProcessDeliveryAsync(
        RabbitMqInboundEndpoint subscribedEndpoint,
        RabbitMqMessageAcknowledgement acknowledgement,
        BasicDeliverEventArgs eventArgs
    )
    {
        var stoppingToken = _stoppingCancellationTokenSource?.Token ?? CancellationToken.None;

        if (stoppingToken.IsCancellationRequested)
        {
            await acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        await using var scope = _serviceScopeFactory.CreateAsyncScope();
        using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            eventArgs.CancellationToken,
            stoppingToken
        );
        var cancellationToken = linkedCancellationTokenSource.Token;
        var transportMessage = new RabbitMqTransportMessage(
            subscribedEndpoint.QueueName,
            eventArgs.ConsumerTag,
            eventArgs.DeliveryTag,
            eventArgs.Redelivered,
            eventArgs.Exchange,
            eventArgs.RoutingKey,
            eventArgs.BasicProperties,
            eventArgs.Body
        );

        try
        {
            var inspector = (IInboundMessageInspector) scope.ServiceProvider.GetRequiredService(
                subscribedEndpoint.InspectorType
            );
            var inspection = await inspector.InspectAsync(transportMessage, cancellationToken)
               .ConfigureAwait(false);

            if (!_topology.TryDispatch(
                    subscribedEndpoint.QueueName,
                    inspection.Discriminator,
                    out var endpoint
                ) ||
                endpoint is null)
            {
                throw new UnknownInboundMessageException(
                    subscribedEndpoint.QueueName,
                    inspection.Discriminator
                );
            }

            if (endpoint.MessageType != inspection.MessageType &&
                !endpoint.MessageType.IsAssignableFrom(inspection.MessageType))
            {
                throw new UnknownInboundMessageException(
                    subscribedEndpoint.QueueName,
                    inspection.Discriminator,
                    $"Inbound message discriminator '{inspection.Discriminator}' resolved to '{inspection.MessageType}', but endpoint '{endpoint.Name}' handles '{endpoint.MessageType}'."
                );
            }

            IncomingMessageContext context = new (
                transportMessage,
                endpoint,
                scope.ServiceProvider,
                acknowledgement,
                cancellationToken
            )
            {
                Message = inspection.Message
            };

            if (inspection.Envelope is CloudEventEnvelope envelope)
            {
                context.SetItem(CloudEventsContextKeys.Envelope, envelope);
            }

            await _topology.Pipeline(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            await acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "RabbitMQ inbound delivery failed for queue {QueueName} and delivery tag {DeliveryTag}",
                subscribedEndpoint.QueueName,
                eventArgs.DeliveryTag
            );
            await acknowledgement.NackAsync(requeue: false, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private sealed record ConsumerRegistration(IChannel Channel, string ConsumerTag);

    private sealed class InFlightDelivery
    {
        private readonly TaskCompletionSource<bool> _completion =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public InFlightDelivery(RabbitMqMessageAcknowledgement acknowledgement)
        {
            Acknowledgement = acknowledgement;
        }

        public RabbitMqMessageAcknowledgement Acknowledgement { get; }

        public Task Completion => _completion.Task;

        public void SetCompleted()
        {
            _completion.TrySetResult(true);
        }
    }
}
