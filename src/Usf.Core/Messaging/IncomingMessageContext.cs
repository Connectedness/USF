using System;
using System.Collections.Generic;
using System.Threading;

namespace Usf.Core.Messaging;

public sealed class IncomingMessageContext
{
    private Dictionary<object, object?>? _items;

    public IncomingMessageContext(
        TransportMessage transport,
        InboundEndpoint endpoint,
        IServiceProvider services,
        IMessageAcknowledgement acknowledgement,
        CancellationToken cancellationToken
    )
    {
        Transport = transport ?? throw new ArgumentNullException(nameof(transport));
        Endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
        Services = services ?? throw new ArgumentNullException(nameof(services));
        Acknowledgement = acknowledgement ?? throw new ArgumentNullException(nameof(acknowledgement));
        CancellationToken = cancellationToken;
    }

    public TransportMessage Transport { get; }

    public InboundEndpoint Endpoint { get; }

    public IServiceProvider Services { get; }

    public object? Message { get; set; }

    public IMessageAcknowledgement Acknowledgement { get; }

    public CancellationToken CancellationToken { get; }

    public void SetItem<T>(MessageContextKey<T> key, T value)
    {
        _items ??= new Dictionary<object, object?>();
        _items[key] = value;
    }

    public bool TryGetItem<T>(MessageContextKey<T> key, out T? value)
    {
        if (_items is not null &&
            _items.TryGetValue(key, out var item) &&
            item is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }

    public T GetRequiredItem<T>(MessageContextKey<T> key)
    {
        if (TryGetItem(key, out var value))
        {
            return value!;
        }

        throw new InvalidOperationException($"Message context item '{key.Name}' is not set.");
    }

    public bool RemoveItem<T>(MessageContextKey<T> key)
    {
        return _items is not null && _items.Remove(key);
    }
}
