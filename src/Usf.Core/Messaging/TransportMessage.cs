using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Usf.Core.Messaging;

public abstract class TransportMessage
{
    /// <summary>
    /// Initializes a transport message and takes ownership of its body and headers.
    /// </summary>
    /// <param name="transportName">The transport name.</param>
    /// <param name="source">The transport-specific source.</param>
    /// <param name="body">
    /// The message body. The caller must not mutate its backing memory after construction.
    /// </param>
    /// <param name="headers">
    /// The message headers. The caller must not mutate the dictionary after construction.
    /// </param>
    /// <param name="contentType">The body content type.</param>
    /// <param name="contentEncoding">The body content encoding.</param>
    /// <param name="messageId">The transport message identifier.</param>
    /// <param name="correlationId">The correlation identifier.</param>
    /// <param name="replyTo">The reply destination.</param>
    /// <param name="timestamp">The message timestamp.</param>
    /// <param name="priority">The message priority.</param>
    /// <param name="timeToLive">The message time to live.</param>
    /// <param name="redelivered">Whether the transport reports this message as redelivered.</param>
    /// <param name="deliveryAttempt">The one-based delivery attempt.</param>
    /// <param name="userId">The producer user identifier.</param>
    /// <param name="appId">The producer application identifier.</param>
    /// <remarks>
    /// The body and headers are stored as passed without defensive copies.
    /// </remarks>
    protected TransportMessage(
        string transportName,
        string source,
        ReadOnlyMemory<byte> body,
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
        Body = body;
        Headers = headers ?? throw new ArgumentNullException(nameof(headers));
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

    /// <summary>
    /// Gets the message body.
    /// </summary>
    /// <remarks>
    /// A transport may expose transport-owned pooled memory. In that case this memory is valid only until the message
    /// handler completes. The message must not be retained and processing must not be offloaded past the handler's
    /// lifetime; violations read reused buffer contents rather than throwing.
    /// </remarks>
    public ReadOnlyMemory<byte> Body { get; }

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
}
