using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace Usf.Core.Messaging;

public abstract class TransportMessage
{
    protected TransportMessage(
        string transportName,
        string source,
        byte[] body,
        IReadOnlyDictionary<string, object?> headers,
        string? contentType = null,
        string? contentEncoding = null,
        string? messageId = null,
        string? correlationId = null,
        string? replyTo = null,
        DateTimeOffset? timestamp = null,
        byte? priority = null,
        TimeSpan? timeToLive = null,
        bool redelivered = false,
        uint deliveryAttempt = 1,
        string? userId = null,
        string? appId = null
    )
    {
        TransportName = RequireText(transportName, nameof(transportName));
        Source = RequireText(source, nameof(source));
        Body = body is null ? throw new ArgumentNullException(nameof(body)) : (byte[]) body.Clone();
        Headers = CopyHeaders(headers ?? throw new ArgumentNullException(nameof(headers)));
        ContentType = contentType;
        ContentEncoding = contentEncoding;
        MessageId = messageId;
        CorrelationId = correlationId;
        ReplyTo = replyTo;
        Timestamp = timestamp;
        Priority = priority;
        TimeToLive = timeToLive;
        Redelivered = redelivered;
        DeliveryAttempt = deliveryAttempt == 0 ? 1 : deliveryAttempt;
        UserId = userId;
        AppId = appId;
    }

    public string TransportName { get; }

    public string Source { get; }

    public byte[] Body { get; }

    public string? ContentType { get; }

    public string? ContentEncoding { get; }

    public string? MessageId { get; }

    public string? CorrelationId { get; }

    public string? ReplyTo { get; }

    public DateTimeOffset? Timestamp { get; }

    public byte? Priority { get; }

    public TimeSpan? TimeToLive { get; }

    public bool Redelivered { get; }

    public uint DeliveryAttempt { get; }

    public string? UserId { get; }

    public string? AppId { get; }

    public IReadOnlyDictionary<string, object?> Headers { get; }

    public bool TryGetHeaderString(string name, out string? value)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!Headers.TryGetValue(name, out var rawValue) || rawValue is null)
        {
            value = null;
            return false;
        }

        value = rawValue switch
        {
            string stringValue => stringValue,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.ToArray()),
            Memory<byte> memory => Encoding.UTF8.GetString(memory.ToArray()),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => rawValue.ToString()
        };
        return true;
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }

    private static IReadOnlyDictionary<string, object?> CopyHeaders(IReadOnlyDictionary<string, object?> headers)
    {
        Dictionary<string, object?> copy = new (headers.Count, StringComparer.Ordinal);

        foreach (var header in headers)
        {
            copy[header.Key] = header.Value;
        }

        return new ReadOnlyDictionary<string, object?>(copy);
    }
}
