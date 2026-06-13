using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public interface ITestRecoverableChannel : IChannel, IRecoverable { }

public sealed class TestRabbitMqChannel
{
    private readonly string _disposalEventName;
    private readonly IList<string>? _disposalEvents;
    private AsyncEventHandler<ShutdownEventArgs>? _channelShutdownAsync;
    private AsyncEventHandler<AsyncEventArgs>? _recoveryAsync;

    public TestRabbitMqChannel(IList<string>? disposalEvents = null, string disposalEventName = "channel")
    {
        Object = RabbitMqDispatchProxy<ITestRecoverableChannel>.Create(HandleInvoke);
        _disposalEvents = disposalEvents;
        _disposalEventName = disposalEventName;
    }

    public Func<CancellationToken, ValueTask>? BasicPublishAsyncHandler { get; set; }

    public int BasicPublishCallCount { get; private set; }

    public ReadOnlyMemory<byte> LastPublishedBody { get; private set; }

    public BasicProperties? LastPublishedProperties { get; private set; }

    public string? LastPublishedRoutingKey { get; private set; }

    public ShutdownEventArgs? CloseReason { get; private set; }

    public int DisposeAsyncCallCount { get; private set; }

    public int DisposeCallCount { get; private set; }

    public int RecoveryAsyncAddCallCount { get; private set; }

    public int RecoveryAsyncRemoveCallCount { get; private set; }

    public int ShutdownAsyncAddCallCount { get; private set; }

    public int ShutdownAsyncRemoveCallCount { get; private set; }

    public bool IsOpen { get; private set; } = true;

    public IChannel Object { get; }

    public void Close(ushort replyCode = 200, string replyText = "Closed")
    {
        IsOpen = false;
        CloseReason = CreateShutdownEvent(replyCode, replyText);
    }

    public async Task ShutdownAsync(ushort replyCode = 200, string replyText = "Closed")
    {
        Close(replyCode, replyText);

        if (_channelShutdownAsync is not null)
        {
            await _channelShutdownAsync(Object, CloseReason!).ConfigureAwait(false);
        }
    }

    public async Task RaiseRecoveryAsync()
    {
        IsOpen = true;
        CloseReason = null;

        if (_recoveryAsync is not null)
        {
            await _recoveryAsync(Object, new AsyncEventArgs(CancellationToken.None)).ConfigureAwait(false);
        }
    }

    private object? HandleInvoke(MethodInfo targetMethod, object?[]? arguments)
    {
        switch (targetMethod.Name)
        {
            case "get_IsOpen":
                return IsOpen;
            case "get_IsClosed":
                return !IsOpen;
            case "get_CloseReason":
                return CloseReason;
            case "add_ChannelShutdownAsync":
                ShutdownAsyncAddCallCount++;
                _channelShutdownAsync += (AsyncEventHandler<ShutdownEventArgs>) arguments![0]!;
                return null;
            case "remove_ChannelShutdownAsync":
                ShutdownAsyncRemoveCallCount++;
                _channelShutdownAsync -= (AsyncEventHandler<ShutdownEventArgs>) arguments![0]!;
                return null;
            case "add_RecoveryAsync":
                RecoveryAsyncAddCallCount++;
                _recoveryAsync += (AsyncEventHandler<AsyncEventArgs>) arguments![0]!;
                return null;
            case "remove_RecoveryAsync":
                RecoveryAsyncRemoveCallCount++;
                _recoveryAsync -= (AsyncEventHandler<AsyncEventArgs>) arguments![0]!;
                return null;
            case "BasicPublishAsync":
                BasicPublishCallCount++;
                LastPublishedRoutingKey = (string) arguments![1]!;
                LastPublishedProperties = (BasicProperties) arguments![3]!;
                LastPublishedBody = (ReadOnlyMemory<byte>) arguments[4]!;
                return BasicPublishAsyncHandler?.Invoke((CancellationToken) arguments![^1]!) ?? default(ValueTask);
            case "DisposeAsync":
                DisposeAsyncCallCount++;
                IsOpen = false;
                _disposalEvents?.Add(_disposalEventName);
                return default(ValueTask);
            case "Dispose":
                DisposeCallCount++;
                IsOpen = false;
                _disposalEvents?.Add(_disposalEventName);
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
