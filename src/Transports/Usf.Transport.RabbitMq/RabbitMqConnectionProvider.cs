using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqConnectionProvider : IAsyncDisposable, IDisposable
{
    private readonly Func<CancellationToken, Task<IConnection>> _createConnectionAsync;
    private readonly SemaphoreSlim _gate = new (1, 1);
    private volatile Task<IConnection>? _connectionTask;
    private int _disposed;

    public RabbitMqConnectionProvider(Func<CancellationToken, Task<IConnection>> createConnectionAsync)
    {
        _createConnectionAsync =
            createConnectionAsync ?? throw new ArgumentNullException(nameof(createConnectionAsync));
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _gate.Dispose();

        if (_connectionTask is not null && _connectionTask.Status == TaskStatus.RanToCompletion)
        {
            var connection = await _connectionTask.ConfigureAwait(false);
            if (connection is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                connection.Dispose();
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _gate.Dispose();

        if (_connectionTask is not null && _connectionTask.Status == TaskStatus.RanToCompletion)
        {
            _connectionTask.Result.Dispose();
        }
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var existingTask = _connectionTask;

        if (existingTask is not null)
        {
            return await existingTask.ConfigureAwait(false);
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            ThrowIfDisposed();

            if (_connectionTask is null)
            {
                _connectionTask = _createConnectionAsync(cancellationToken);
            }

            return await _connectionTask.ConfigureAwait(false);
        }
        catch
        {
            // A failed creation attempt must not poison the provider: clear the cached task so the
            // next acquisition retries instead of replaying the same faulted task forever.
            _connectionTask = null;
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));
        }
    }
}
