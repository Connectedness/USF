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
        DeclareMode = RabbitMqDeclareMode.Active;
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
}
