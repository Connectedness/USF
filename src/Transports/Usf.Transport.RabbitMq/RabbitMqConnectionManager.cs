using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging.Errors;

namespace Usf.Transport.RabbitMq;

internal sealed class RabbitMqConnectionManager : IAsyncDisposable, IDisposable
{
    private readonly Func<IServiceProvider, ConnectionFactory> _connectionFactoryFactory;
    private readonly SemaphoreSlim _gate = new (1, 1);
    private readonly IServiceProvider _serviceProvider;
    private Task<IConnection>? _connectionTask;
    private bool _disposed;

    public RabbitMqConnectionManager(RabbitMqPublishingConfiguration configuration, IServiceProvider serviceProvider)
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        _connectionFactoryFactory = configuration.ConnectionFactoryFactory ??
                                    throw new MessageTopologyValidationException(
                                        ["A RabbitMQ connection factory must be configured."]
                                    );
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
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
        if (_disposed)
        {
            return;
        }

        _disposed = true;
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
                var connectionFactory = _connectionFactoryFactory(_serviceProvider);
                _connectionTask = connectionFactory.CreateConnectionAsync(cancellationToken);
            }

            return await _connectionTask.ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMqConnectionManager));
        }
    }
}
