using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class TestRabbitMqConnection
{
    private readonly Queue<IChannel> _channels = new ();
    private AsyncEventHandler<ConnectionRecoveryErrorEventArgs>? _connectionRecoveryErrorAsync;
    private AsyncEventHandler<ShutdownEventArgs>? _connectionShutdownAsync;
    private AsyncEventHandler<AsyncEventArgs>? _recoverySucceededAsync;

    public TestRabbitMqConnection(IList<string>? disposalEvents = null, string disposalEventName = "connection")
    {
        Object = RabbitMqDispatchProxy<IConnection>.Create(HandleInvoke);
        DisposalEvents = disposalEvents;
        DisposalEventName = disposalEventName;
    }

    public ushort ChannelMax { get; set; }

    public ShutdownEventArgs? CloseReason { get; private set; }

    public int ConnectionRecoveryErrorAsyncAddCallCount { get; private set; }

    public int ConnectionRecoveryErrorAsyncRemoveCallCount { get; private set; }

    public int ConnectionShutdownAsyncAddCallCount { get; private set; }

    public int ConnectionShutdownAsyncRemoveCallCount { get; private set; }

    public int DisposeAsyncCallCount { get; private set; }

    public int DisposeCallCount { get; private set; }

    public IList<CreateChannelOptions?> CreateChannelOptions { get; } = new List<CreateChannelOptions?>();

    public IList<string>? DisposalEvents { get; }

    public string DisposalEventName { get; }

    public bool IsOpen { get; private set; } = true;

    public int RecoverySucceededAsyncAddCallCount { get; private set; }

    public int RecoverySucceededAsyncRemoveCallCount { get; private set; }

    public IConnection Object { get; }

    public void EnqueueChannel(IChannel channel)
    {
        _channels.Enqueue(channel ?? throw new ArgumentNullException(nameof(channel)));
    }

    public async Task RaiseConnectionRecoveryErrorAsync(Exception exception)
    {
        if (_connectionRecoveryErrorAsync is not null)
        {
            await _connectionRecoveryErrorAsync(
                    Object,
                    new ConnectionRecoveryErrorEventArgs(exception, CancellationToken.None)
                )
               .ConfigureAwait(false);
        }
    }

    public async Task RaiseConnectionShutdownAsync(ushort replyCode = 200, string replyText = "Closed")
    {
        IsOpen = false;
        CloseReason = CreateShutdownEvent(replyCode, replyText);

        if (_connectionShutdownAsync is not null)
        {
            await _connectionShutdownAsync(Object, CloseReason).ConfigureAwait(false);
        }
    }

    public async Task RaiseRecoverySucceededAsync()
    {
        IsOpen = true;
        CloseReason = null;

        if (_recoverySucceededAsync is not null)
        {
            await _recoverySucceededAsync(Object, new AsyncEventArgs(CancellationToken.None)).ConfigureAwait(false);
        }
    }

    private object? HandleInvoke(MethodInfo targetMethod, object?[]? arguments)
    {
        switch (targetMethod.Name)
        {
            case "get_ChannelMax":
                return ChannelMax;
            case "get_IsOpen":
                return IsOpen;
            case "get_CloseReason":
                return CloseReason;
            case "add_ConnectionShutdownAsync":
                ConnectionShutdownAsyncAddCallCount++;
                _connectionShutdownAsync += (AsyncEventHandler<ShutdownEventArgs>) arguments![0]!;
                return null;
            case "remove_ConnectionShutdownAsync":
                ConnectionShutdownAsyncRemoveCallCount++;
                _connectionShutdownAsync -= (AsyncEventHandler<ShutdownEventArgs>) arguments![0]!;
                return null;
            case "add_RecoverySucceededAsync":
                RecoverySucceededAsyncAddCallCount++;
                _recoverySucceededAsync += (AsyncEventHandler<AsyncEventArgs>) arguments![0]!;
                return null;
            case "remove_RecoverySucceededAsync":
                RecoverySucceededAsyncRemoveCallCount++;
                _recoverySucceededAsync -= (AsyncEventHandler<AsyncEventArgs>) arguments![0]!;
                return null;
            case "add_ConnectionRecoveryErrorAsync":
                ConnectionRecoveryErrorAsyncAddCallCount++;
                _connectionRecoveryErrorAsync += (AsyncEventHandler<ConnectionRecoveryErrorEventArgs>) arguments![0]!;
                return null;
            case "remove_ConnectionRecoveryErrorAsync":
                ConnectionRecoveryErrorAsyncRemoveCallCount++;
                _connectionRecoveryErrorAsync -= (AsyncEventHandler<ConnectionRecoveryErrorEventArgs>) arguments![0]!;
                return null;
            case "CreateChannelAsync":
                CreateChannelOptions.Add((CreateChannelOptions?) arguments![0]);
                return Task.FromResult(_channels.Dequeue());
            case "DisposeAsync":
                DisposeAsyncCallCount++;
                IsOpen = false;
                DisposalEvents?.Add(DisposalEventName);
                return default(ValueTask);
            case "Dispose":
                DisposeCallCount++;
                IsOpen = false;
                DisposalEvents?.Add(DisposalEventName);
                return null;
        }

        if (targetMethod.Name.StartsWith("add_", StringComparison.Ordinal) ||
            targetMethod.Name.StartsWith("remove_", StringComparison.Ordinal))
        {
            return null;
        }

        return RabbitMqDispatchProxyDefaults.GetDefaultValue(targetMethod.ReturnType);
    }

    private static ShutdownEventArgs CreateShutdownEvent(ushort replyCode, string replyText)
    {
        return new ShutdownEventArgs(
            ShutdownInitiator.Library,
            replyCode,
            replyText,
            new object(),
            CancellationToken.None
        );
    }
}
