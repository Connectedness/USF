using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqQueueBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    public RabbitMqQueueBuilder(string name)
    {
        Name = RequireText(name, nameof(name));
        DeclareMode = RabbitMqDeclareMode.Ensure;
        Durable = true;
    }

    public string Name { get; }

    public RabbitMqDeclareMode DeclareMode { get; private set; }

    public bool Durable { get; private set; }

    public bool Exclusive { get; private set; }

    public bool AutoDelete { get; private set; }

    public RabbitMqQueueBuilder WithDeclareMode(RabbitMqDeclareMode declareMode)
    {
        DeclareMode = declareMode;
        return this;
    }

    public RabbitMqQueueBuilder DurableQueue(bool durable = true)
    {
        Durable = durable;
        return this;
    }

    public RabbitMqQueueBuilder ExclusiveQueue(bool exclusive = true)
    {
        Exclusive = exclusive;
        return this;
    }

    public RabbitMqQueueBuilder AutoDeleteQueue(bool autoDelete = true)
    {
        AutoDelete = autoDelete;
        return this;
    }

    public RabbitMqQueueBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    public RabbitMqQueueBuilder WithDeadLetterExchange(string exchangeName)
    {
        _arguments["x-dead-letter-exchange"] = RequireText(exchangeName, nameof(exchangeName));
        return this;
    }

    public RabbitMqQueueBuilder WithDeadLetterRoutingKey(string routingKey)
    {
        _arguments["x-dead-letter-routing-key"] = routingKey ?? string.Empty;
        return this;
    }

    public RabbitMqQueueBuilder WithMessageTtl(TimeSpan timeToLive)
    {
        _arguments["x-message-ttl"] = ToMilliseconds(timeToLive, nameof(timeToLive));
        return this;
    }

    public RabbitMqQueueBuilder WithExpires(TimeSpan expires)
    {
        _arguments["x-expires"] = ToMilliseconds(expires, nameof(expires));
        return this;
    }

    public RabbitMqQueueBuilder WithMaxLength(long maxLength)
    {
        if (maxLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLength), "The value must be zero or greater.");
        }

        _arguments["x-max-length"] = maxLength;
        return this;
    }

    public RabbitMqQueueBuilder WithMaxLengthBytes(long maxLengthBytes)
    {
        if (maxLengthBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLengthBytes), "The value must be zero or greater.");
        }

        _arguments["x-max-length-bytes"] = maxLengthBytes;
        return this;
    }

    public RabbitMqQueueBuilder WithQueueType(string queueType)
    {
        _arguments["x-queue-type"] = RequireText(queueType, nameof(queueType));
        return this;
    }

    public RabbitMqQueueBuilder AsQuorumQueue()
    {
        _arguments["x-queue-type"] = "quorum";
        return this;
    }

    public RabbitMqQueueBuilder SingleActiveConsumer(bool singleActiveConsumer = true)
    {
        _arguments["x-single-active-consumer"] = singleActiveConsumer;
        return this;
    }

    internal RabbitMqQueueDefinition Build()
    {
        return new RabbitMqQueueDefinition(
            Name,
            DeclareMode,
            Durable,
            Exclusive,
            AutoDelete,
            new ReadOnlyDictionary<string, object?>(_arguments)
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

    private static long ToMilliseconds(TimeSpan value, string parameterName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, "The value must be zero or greater.");
        }

        return checked((long) value.TotalMilliseconds);
    }
}
