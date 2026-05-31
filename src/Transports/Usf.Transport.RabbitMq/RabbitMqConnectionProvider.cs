using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqConnectionProvider : IAsyncDisposable, IDisposable
{
    private readonly Func<CancellationToken, Task<IConnection>> _createConnectionAsync;
    private readonly SemaphoreSlim _gate = new (1, 1);
    private readonly ILogger _logger;
    private volatile Task<IConnection>? _connectionTask;
    private int _disposed;

    public RabbitMqConnectionProvider(
        Func<CancellationToken, Task<IConnection>> createConnectionAsync,
        ILogger? logger = null
    )
    {
        _createConnectionAsync =
            createConnectionAsync ?? throw new ArgumentNullException(nameof(createConnectionAsync));
        _logger = logger ?? NullLogger.Instance;
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
            Unsubscribe(connection);

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
            var connection = _connectionTask.Result;
            Unsubscribe(connection);
            connection.Dispose();
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
                _connectionTask = CreateConnectionAsync(cancellationToken);
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

    private async Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = await _createConnectionAsync(cancellationToken).ConfigureAwait(false);
        Subscribe(connection);
        return connection;
    }

    private Task OnConnectionRecoveryErrorAsync(object sender, ConnectionRecoveryErrorEventArgs eventArgs)
    {
        _logger.LogWarning(
            eventArgs.Exception,
            "RabbitMQ connection lifecycle transition {Transition}",
            "recovery-failed"
        );
        return Task.CompletedTask;
    }

    private Task OnConnectionShutdownAsync(object sender, ShutdownEventArgs eventArgs)
    {
        _logger.LogWarning(
            "RabbitMQ connection lifecycle transition {Transition}: initiator {Initiator}, reply code {ReplyCode}, reply text {ReplyText}",
            "shutdown",
            eventArgs.Initiator,
            eventArgs.ReplyCode,
            eventArgs.ReplyText
        );
        return Task.CompletedTask;
    }

    private Task OnRecoverySucceededAsync(object sender, AsyncEventArgs eventArgs)
    {
        _logger.LogInformation(
            "RabbitMQ connection lifecycle transition {Transition}",
            "recovery-succeeded"
        );
        return Task.CompletedTask;
    }

    private void Subscribe(IConnection connection)
    {
        connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;
        connection.RecoverySucceededAsync += OnRecoverySucceededAsync;
        connection.ConnectionRecoveryErrorAsync += OnConnectionRecoveryErrorAsync;
    }

    private void Unsubscribe(IConnection connection)
    {
        connection.ConnectionShutdownAsync -= OnConnectionShutdownAsync;
        connection.RecoverySucceededAsync -= OnRecoverySucceededAsync;
        connection.ConnectionRecoveryErrorAsync -= OnConnectionRecoveryErrorAsync;
    }
}
