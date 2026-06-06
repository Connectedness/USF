using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging.Errors;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqChannelSource : IDisposable
{
    private readonly SemaphoreSlim _channelBudgetValidationGate = new (1, 1);
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly Func<IReadOnlyList<string>, Exception> _createValidationException;
    private readonly string _topologyDirection;
    private int _channelBudgetConfigured;
    private int _channelBudgetValidated;
    private int _worstCaseChannelCount;
    private string _worstCaseChannelCountDescription = string.Empty;

    public RabbitMqChannelSource(
        RabbitMqConnectionProvider connectionProvider,
        string topologyDirection = "outbound",
        Func<IReadOnlyList<string>, Exception>? createValidationException = null
    )
    {
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _topologyDirection = RequireText(topologyDirection, nameof(topologyDirection));
        _createValidationException = createValidationException ??
                                     (static validationErrors =>
                                         new OutboundTopologyValidationException(validationErrors));
    }

    public void Dispose()
    {
        _channelBudgetValidationGate.Dispose();
    }

    public void SetChannelBudget(int worstCaseChannelCount, string worstCaseChannelCountDescription)
    {
        if (Interlocked.Exchange(ref _channelBudgetConfigured, 1) != 0)
        {
            throw new InvalidOperationException("The RabbitMQ channel budget can only be configured once.");
        }

        _worstCaseChannelCount = worstCaseChannelCount;
        _worstCaseChannelCountDescription =
            worstCaseChannelCountDescription ??
            throw new ArgumentNullException(nameof(worstCaseChannelCountDescription));
    }

    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<IChannel> CreateChannelAsync(
        CreateChannelOptions? options,
        CancellationToken cancellationToken = default
    )
    {
        if (options is null)
        {
            return await CreateChannelAsync(cancellationToken).ConfigureAwait(false);
        }

        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = await _connectionProvider.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await ValidateChannelBudgetOnceAsync(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private async Task ValidateChannelBudgetOnceAsync(IConnection connection, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _channelBudgetValidated) != 0)
        {
            return;
        }

        await _channelBudgetValidationGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (Volatile.Read(ref _channelBudgetValidated) != 0)
            {
                return;
            }

            ValidateChannelBudget(connection);
            Volatile.Write(ref _channelBudgetValidated, 1);
        }
        finally
        {
            _channelBudgetValidationGate.Release();
        }
    }

    private void ValidateChannelBudget(IConnection connection)
    {
        if (_worstCaseChannelCount == 0 || connection.ChannelMax == 0)
        {
            return;
        }

        if (_worstCaseChannelCount <= connection.ChannelMax)
        {
            return;
        }

        throw _createValidationException(
            new List<string>
            {
                $"RabbitMQ {_topologyDirection} topology may open up to {_worstCaseChannelCount} channels ({_worstCaseChannelCountDescription}), but the broker negotiated channel_max={connection.ChannelMax}."
            }
        );
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
