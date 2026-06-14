using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using RabbitMQ.Client;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqTransportMessage : TransportMessage
{
    private static readonly IReadOnlyDictionary<string, object?> EmptyHeaders =
        new Dictionary<string, object?>(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a RabbitMQ transport message.
    /// </summary>
    /// <param name="queueName">The source queue name.</param>
    /// <param name="consumerTag">The RabbitMQ consumer tag.</param>
    /// <param name="deliveryTag">The RabbitMQ delivery tag.</param>
    /// <param name="redelivered">Whether RabbitMQ reports this message as redelivered.</param>
    /// <param name="exchange">The source exchange.</param>
    /// <param name="routingKey">The delivery routing key.</param>
    /// <param name="basicProperties">The RabbitMQ basic properties.</param>
    /// <param name="body">The RabbitMQ delivery body.</param>
    /// <param name="copyBody">
    /// <see langword="true" /> to copy the body into message-owned memory; <see langword="false" /> to reference
    /// RabbitMQ.Client's pooled delivery buffer. When <see langword="false" />, the body and values derived from it
    /// without copying are valid only until the message handler completes. The message must not be retained and
    /// processing must not be offloaded past the handler's lifetime; violations read reused buffer contents rather
    /// than throwing.
    /// </param>
    public RabbitMqTransportMessage(
        string queueName,
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties basicProperties,
        ReadOnlyMemory<byte> body,
        bool copyBody = true
    )
        : base(
            "rabbitmq",
            queueName,
            copyBody ? body.ToArray() : body,
            GetHeaders(basicProperties),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsContentTypePresent(),
                static properties => properties.ContentType
            ),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsContentEncodingPresent(),
                static properties => properties.ContentEncoding
            ),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsMessageIdPresent(),
                static properties => properties.MessageId
            ),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsCorrelationIdPresent(),
                static properties => properties.CorrelationId
            ),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsReplyToPresent(),
                static properties => properties.ReplyTo
            ),
            GetTimestamp(basicProperties),
            GetPriority(basicProperties),
            GetTimeToLive(basicProperties),
            redelivered,
            GetDeliveryAttempt(basicProperties, redelivered),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsUserIdPresent(),
                static properties => properties.UserId
            ),
            GetPropertyValue(
                basicProperties,
                static properties => properties.IsAppIdPresent(),
                static properties => properties.AppId
            )
        )
    {
        ConsumerTag = consumerTag;
        DeliveryTag = deliveryTag;
        Exchange = exchange;
        RoutingKey = routingKey;
        BasicProperties = basicProperties ?? throw new ArgumentNullException(nameof(basicProperties));
        DeliveryMode = basicProperties.DeliveryMode;
    }

    public ulong DeliveryTag { get; }

    public string Exchange { get; }

    public string RoutingKey { get; }

    public string ConsumerTag { get; }

    public DeliveryModes DeliveryMode { get; }

    public IReadOnlyBasicProperties BasicProperties { get; }

    private static string? GetPropertyValue(
        IReadOnlyBasicProperties basicProperties,
        Func<IReadOnlyBasicProperties, bool> isPresent,
        Func<IReadOnlyBasicProperties, string?> getValue
    )
    {
        return isPresent(basicProperties) ? getValue(basicProperties) : null;
    }

    private static DateTimeOffset? GetTimestamp(IReadOnlyBasicProperties basicProperties)
    {
        return basicProperties.IsTimestampPresent() ?
            DateTimeOffset.FromUnixTimeSeconds(basicProperties.Timestamp.UnixTime) :
            null;
    }

    private static byte? GetPriority(IReadOnlyBasicProperties basicProperties)
    {
        return basicProperties.IsPriorityPresent() ? basicProperties.Priority : null;
    }

    private static TimeSpan? GetTimeToLive(IReadOnlyBasicProperties basicProperties)
    {
        if (!basicProperties.IsExpirationPresent() ||
            !long.TryParse(
                basicProperties.Expiration,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var milliseconds
            ))
        {
            return null;
        }

        return TimeSpan.FromMilliseconds(milliseconds);
    }

    private static IReadOnlyDictionary<string, object?> GetHeaders(IReadOnlyBasicProperties basicProperties)
    {
        if (!basicProperties.IsHeadersPresent() || basicProperties.Headers is null)
        {
            return EmptyHeaders;
        }

        return basicProperties.Headers as IReadOnlyDictionary<string, object?> ??
               new ReadOnlyDictionary<string, object?>(basicProperties.Headers);
    }

    private static uint GetDeliveryAttempt(IReadOnlyBasicProperties basicProperties, bool redelivered)
    {
        if (!basicProperties.IsHeadersPresent() || basicProperties.Headers is not { } headers)
        {
            return redelivered ? 2u : 1u;
        }

        if (TryGetUnsignedHeader(headers, "x-delivery-count", out var deliveryCount))
        {
            return checked(deliveryCount + 1);
        }

        if (headers.TryGetValue("x-death", out var rawDeath) &&
            TryGetDeathCount(rawDeath, out var deathCount))
        {
            return checked(deathCount + 1);
        }

        return redelivered ? 2u : 1u;
    }

    private static bool TryGetUnsignedHeader(
        IDictionary<string, object?> headers,
        string name,
        out uint value
    )
    {
        if (!headers.TryGetValue(name, out var rawValue))
        {
            value = 0;
            return false;
        }

        return TryConvertToUInt32(rawValue, out value);
    }

    private static bool TryGetDeathCount(object? rawDeath, out uint value)
    {
        if (rawDeath is IEnumerable enumerable)
        {
            foreach (var item in enumerable)
            {
                if (item is not IDictionary<string, object?> death ||
                    !death.TryGetValue("count", out var rawCount) ||
                    !TryConvertToUInt32(rawCount, out value))
                {
                    continue;
                }

                return true;
            }
        }

        value = 0;
        return false;
    }

    private static bool TryConvertToUInt32(object? rawValue, out uint value)
    {
        switch (rawValue)
        {
            case byte byteValue:
                value = byteValue;
                return true;
            case sbyte sbyteValue when sbyteValue >= 0:
                value = (uint) sbyteValue;
                return true;
            case short shortValue when shortValue >= 0:
                value = (uint) shortValue;
                return true;
            case ushort ushortValue:
                value = ushortValue;
                return true;
            case int intValue when intValue >= 0:
                value = (uint) intValue;
                return true;
            case uint uintValue:
                value = uintValue;
                return true;
            case long longValue and >= 0 and <= uint.MaxValue:
                value = (uint) longValue;
                return true;
            case ulong ulongValue when ulongValue <= uint.MaxValue:
                value = (uint) ulongValue;
                return true;
            default:
                value = 0;
                return false;
        }
    }
}
