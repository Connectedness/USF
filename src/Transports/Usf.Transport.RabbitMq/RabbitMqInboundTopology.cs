using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundTopology : IAsyncDisposable, IDisposable
{
    private readonly RabbitMqChannelSource _channelSource;
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly IReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> _dispatchIndex;
    private int _disposed;

    public RabbitMqInboundTopology(
        InboundTopology inboundTopology,
        IMessageContractRegistry messageContractRegistry,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<RabbitMqInboundChannelGroup> channelGroups,
        IReadOnlyList<RabbitMqInboundEndpoint> endpoints,
        IReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> dispatchIndex,
        MessageDelegate pipeline,
        TimeSpan shutdownTimeout,
        RabbitMqConnectionProvider connectionProvider,
        RabbitMqChannelSource channelSource
    )
    {
        InboundTopology = inboundTopology ?? throw new ArgumentNullException(nameof(inboundTopology));
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
        ChannelGroups = channelGroups ?? throw new ArgumentNullException(nameof(channelGroups));
        Endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _dispatchIndex = dispatchIndex ?? throw new ArgumentNullException(nameof(dispatchIndex));
        Pipeline = pipeline ?? throw new ArgumentNullException(nameof(pipeline));
        ShutdownTimeout = shutdownTimeout;
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _channelSource = channelSource ?? throw new ArgumentNullException(nameof(channelSource));
    }

    public InboundTopology InboundTopology { get; }

    public IMessageContractRegistry MessageContractRegistry { get; }

    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }

    public IReadOnlyList<RabbitMqInboundChannelGroup> ChannelGroups { get; }

    public IReadOnlyList<RabbitMqInboundEndpoint> Endpoints { get; }

    public MessageDelegate Pipeline { get; }

    public TimeSpan ShutdownTimeout { get; }

    public IEnumerable<IGrouping<RabbitMqInboundChannelGroup, RabbitMqInboundEndpoint>> EndpointsByChannelGroup =>
        Endpoints.GroupBy(static endpoint => endpoint.ChannelGroup);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
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

    public bool TryDispatch(string queueName, string discriminator, out RabbitMqInboundEndpoint? endpoint)
    {
        return _dispatchIndex.TryGetValue(new InboundEndpointSelectionKey(queueName, discriminator), out endpoint);
    }
}
