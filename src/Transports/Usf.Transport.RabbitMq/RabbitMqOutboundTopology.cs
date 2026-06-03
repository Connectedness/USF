using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqOutboundTopology : IAsyncDisposable, IDisposable
{
    private readonly RabbitMqChannelSource _channelSource;
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private int _disposed;

    public RabbitMqOutboundTopology(
        OutboundTopology outboundTopology,
        IMessageContractRegistry messageContractRegistry,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<RabbitMqAddressDefinition> addresses,
        IReadOnlyList<RabbitMqChannelGroup> channelGroups,
        IReadOnlyList<OutboundTarget> targets,
        RabbitMqConnectionProvider connectionProvider,
        RabbitMqChannelSource channelSource
    )
    {
        OutboundTopology = outboundTopology ?? throw new ArgumentNullException(nameof(outboundTopology));
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
        Addresses = addresses ?? throw new ArgumentNullException(nameof(addresses));
        ChannelGroups = channelGroups ?? throw new ArgumentNullException(nameof(channelGroups));
        Targets = targets ?? throw new ArgumentNullException(nameof(targets));
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _channelSource = channelSource ?? throw new ArgumentNullException(nameof(channelSource));
    }

    public OutboundTopology OutboundTopology { get; }

    public IMessageContractRegistry MessageContractRegistry { get; }

    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }

    public IReadOnlyList<RabbitMqAddressDefinition> Addresses { get; }

    public IReadOnlyList<RabbitMqChannelGroup> ChannelGroups { get; }

    public IReadOnlyList<OutboundTarget> Targets { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in ChannelGroups)
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

        foreach (var channelGroup in ChannelGroups)
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
}
