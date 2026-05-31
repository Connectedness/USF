using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq;

public sealed class DefaultRabbitMqChannelPool : IRabbitMqChannelPool
{
    private readonly TaskCompletionSource<object?> _allChannelsDisposed = new (
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private readonly Channel<PooledChannel> _availableChannels;
    private readonly Func<CancellationToken, Task<IChannel>> _channelFactory;
    private readonly int _maximumChannelCount;
    private int _disposed;
    private int _liveChannelCount;

    public DefaultRabbitMqChannelPool(int maximumChannelCount, Func<CancellationToken, Task<IChannel>> channelFactory)
    {
        if (maximumChannelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumChannelCount),
                maximumChannelCount,
                "The value must be greater than zero."
            );
        }

        _maximumChannelCount = maximumChannelCount;
        _channelFactory = channelFactory ?? throw new ArgumentNullException(nameof(channelFactory));
        _availableChannels = Channel.CreateBounded<PooledChannel>(
            new BoundedChannelOptions(maximumChannelCount)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            }
        );
    }

    public async ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        while (true)
        {
            if (_availableChannels.Reader.TryRead(out var pooledChannel))
            {
                if (!pooledChannel.IsHealthy)
                {
                    await DiscardAsync(pooledChannel).ConfigureAwait(false);
                    continue;
                }

                return CreateLease(pooledChannel);
            }

            if (TryReserveChannelSlot())
            {
                PooledChannel? createdChannel = null;

                try
                {
                    var channel = await _channelFactory(cancellationToken).ConfigureAwait(false);
                    createdChannel = new PooledChannel(channel);

                    if (!createdChannel.IsHealthy)
                    {
                        await DiscardAsync(createdChannel).ConfigureAwait(false);
                        continue;
                    }

                    return CreateLease(createdChannel);
                }
                catch
                {
                    if (createdChannel is null)
                    {
                        SignalChannelDisposed();
                    }

                    throw;
                }
            }

            try
            {
                pooledChannel = await _availableChannels.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException) when (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(DefaultRabbitMqChannelPool));
            }

            if (!pooledChannel.IsHealthy)
            {
                await DiscardAsync(pooledChannel).ConfigureAwait(false);
                continue;
            }

            return CreateLease(pooledChannel);
        }
    }

    public ValueTask ReleaseAsync(in RabbitMqChannelLease lease)
    {
        if (lease.State is not PooledChannel pooledChannel)
        {
            return default;
        }

        if (!pooledChannel.TryCompleteLease(lease.Token))
        {
            return default;
        }

        if (Volatile.Read(ref _disposed) != 0 ||
            !pooledChannel.IsHealthy ||
            !_availableChannels.Writer.TryWrite(pooledChannel))
        {
            return DiscardAsync(pooledChannel);
        }

        return default;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _availableChannels.Writer.TryComplete();

            while (_availableChannels.Reader.TryRead(out var pooledChannel))
            {
                await DiscardAsync(pooledChannel).ConfigureAwait(false);
            }

            if (Volatile.Read(ref _liveChannelCount) == 0)
            {
                _allChannelsDisposed.TrySetResult(null);
            }
        }

        await _allChannelsDisposed.Task.ConfigureAwait(false);
    }

    public void Dispose()
    {
        Task.Run(async () => await DisposeAsync().ConfigureAwait(false)).GetAwaiter().GetResult();
    }

    private RabbitMqChannelLease CreateLease(PooledChannel pooledChannel)
    {
        var leaseId = pooledChannel.BeginLease();
        return new RabbitMqChannelLease(this, pooledChannel.Channel, pooledChannel, leaseId);
    }

    private async ValueTask DiscardAsync(PooledChannel pooledChannel)
    {
        try
        {
            await pooledChannel.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            SignalChannelDisposed();
        }
    }

    private void SignalChannelDisposed()
    {
        if (Interlocked.Decrement(ref _liveChannelCount) == 0 && Volatile.Read(ref _disposed) != 0)
        {
            _allChannelsDisposed.TrySetResult(null);
        }
    }

    private bool TryReserveChannelSlot()
    {
        while (true)
        {
            ThrowIfDisposed();

            var current = Volatile.Read(ref _liveChannelCount);

            if (current >= _maximumChannelCount)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _liveChannelCount, current + 1, current) == current)
            {
                return true;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(DefaultRabbitMqChannelPool));
        }
    }

    public sealed class PooledChannel : IAsyncDisposable
    {
        private readonly IRecoverable? _recoverableChannel;
        private long _currentLeaseId;
        private long _nextLeaseId;
        private int _observedShutdown;

        public PooledChannel(IChannel channel)
        {
            Channel = channel ?? throw new ArgumentNullException(nameof(channel));
            Channel.ChannelShutdownAsync += OnChannelShutdownAsync;

            if (Channel is IRecoverable recoverableChannel)
            {
                _recoverableChannel = recoverableChannel;
                _recoverableChannel.RecoveryAsync += OnRecoveryAsync;
            }
        }

        public IChannel Channel { get; }

        public bool IsHealthy => Volatile.Read(ref _observedShutdown) == 0 && Channel.IsOpen;

        public async ValueTask DisposeAsync()
        {
            Channel.ChannelShutdownAsync -= OnChannelShutdownAsync;
            if (_recoverableChannel is not null)
            {
                _recoverableChannel.RecoveryAsync -= OnRecoveryAsync;
            }

            if (Channel is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                return;
            }

            Channel.Dispose();
        }

        public long BeginLease()
        {
            var leaseId = Interlocked.Increment(ref _nextLeaseId);
            Volatile.Write(ref _currentLeaseId, leaseId);
            return leaseId;
        }

        public bool TryCompleteLease(long leaseId)
        {
            return Interlocked.CompareExchange(ref _currentLeaseId, 0, leaseId) == leaseId;
        }

        private Task OnChannelShutdownAsync(object sender, ShutdownEventArgs eventArgs)
        {
            Interlocked.Exchange(ref _observedShutdown, 1);
            return Task.CompletedTask;
        }

        private Task OnRecoveryAsync(object sender, AsyncEventArgs eventArgs)
        {
            if (Channel.IsOpen)
            {
                Interlocked.Exchange(ref _observedShutdown, 0);
            }

            return Task.CompletedTask;
        }
    }
}
