using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// The compiled RabbitMQ topology. It holds one Core <see cref="Usf.Core.Messaging.Topology" /> projection plus
/// RabbitMQ-specific runtime state: exchanges, queues, bindings, addresses, outbound channel groups, inbound
/// channel groups, outbound targets, inbound endpoints, the inbound pipeline, the shutdown timeout, the
/// connection provider, and the channel source. A topology owns exactly one
/// <see cref="RabbitMqConnectionProvider" />; register separate topology instances when separate publisher and
/// consumer connections are wanted.
/// </summary>
public sealed class RabbitMqTopology : IAsyncDisposable, IDisposable
{
    private readonly RabbitMqChannelSource _channelSource;
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly IReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> _dispatchIndex;
    private int _disposed;

    public RabbitMqTopology(
        Topology topology,
        IMessageContractRegistry messageContractRegistry,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<RabbitMqAddressDefinition> addresses,
        IReadOnlyList<RabbitMqChannelGroup> outboundChannelGroups,
        IReadOnlyList<OutboundTarget> targets,
        IReadOnlyList<RabbitMqInboundChannelGroup> inboundChannelGroups,
        IReadOnlyList<RabbitMqInboundEndpoint> endpoints,
        IReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> dispatchIndex,
        MessageDelegate pipeline,
        TimeSpan shutdownTimeout,
        RabbitMqConnectionProvider connectionProvider,
        RabbitMqChannelSource channelSource
    )
    {
        Topology = topology ?? throw new ArgumentNullException(nameof(topology));
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
        Addresses = addresses ?? throw new ArgumentNullException(nameof(addresses));
        OutboundChannelGroups = outboundChannelGroups ?? throw new ArgumentNullException(nameof(outboundChannelGroups));
        Targets = targets ?? throw new ArgumentNullException(nameof(targets));
        InboundChannelGroups = inboundChannelGroups ?? throw new ArgumentNullException(nameof(inboundChannelGroups));
        Endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _dispatchIndex = dispatchIndex ?? throw new ArgumentNullException(nameof(dispatchIndex));
        Pipeline = pipeline ?? throw new ArgumentNullException(nameof(pipeline));
        ShutdownTimeout = shutdownTimeout;
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _channelSource = channelSource ?? throw new ArgumentNullException(nameof(channelSource));
    }

    public Topology Topology { get; }

    public IMessageContractRegistry MessageContractRegistry { get; }

    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }

    public IReadOnlyList<RabbitMqAddressDefinition> Addresses { get; }

    public IReadOnlyList<RabbitMqChannelGroup> OutboundChannelGroups { get; }

    public IReadOnlyList<OutboundTarget> Targets { get; }

    public IReadOnlyList<RabbitMqInboundChannelGroup> InboundChannelGroups { get; }

    public IReadOnlyList<RabbitMqInboundEndpoint> Endpoints { get; }

    public MessageDelegate Pipeline { get; }

    public TimeSpan ShutdownTimeout { get; }

    public bool HasInboundEndpoints => Endpoints.Count > 0;

    public IEnumerable<IGrouping<RabbitMqInboundChannelGroup, RabbitMqInboundEndpoint>> EndpointsByChannelGroup =>
        Endpoints.GroupBy(static endpoint => endpoint.ChannelGroup);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in OutboundChannelGroups)
        {
            await channelGroup.DisposeAsync().ConfigureAwait(false);
        }

        _channelSource.Dispose();
        await _connectionProvider.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in OutboundChannelGroups)
        {
            channelGroup.Dispose();
        }

        _channelSource.Dispose();
        _connectionProvider.Dispose();
    }

    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        return _channelSource.CreateChannelAsync(cancellationToken);
    }

    public Task<IChannel> CreateChannelAsync(
        CreateChannelOptions? options,
        CancellationToken cancellationToken = default
    )
    {
        return _channelSource.CreateChannelAsync(options, cancellationToken);
    }

    public Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        return _channelSource.GetConnectionAsync(cancellationToken);
    }

    public bool TryDispatch(string queueName, string discriminator, out RabbitMqInboundEndpoint? endpoint)
    {
        return _dispatchIndex.TryGetValue(new InboundEndpointSelectionKey(queueName, discriminator), out endpoint);
    }
}
