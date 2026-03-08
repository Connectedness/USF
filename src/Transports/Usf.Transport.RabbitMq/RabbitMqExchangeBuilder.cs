using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqExchangeBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    public RabbitMqExchangeBuilder(string name, string type)
    {
        Name = RequireText(name, nameof(name));
        Type = RequireText(type, nameof(type));
        DeclareMode = RabbitMqDeclareMode.Active;
        Durable = true;
    }

    public string Name { get; }

    public string Type { get; }

    public RabbitMqDeclareMode DeclareMode { get; private set; }

    public bool Durable { get; private set; }

    public bool AutoDelete { get; private set; }

    public RabbitMqExchangeBuilder WithDeclareMode(RabbitMqDeclareMode declareMode)
    {
        DeclareMode = declareMode;
        return this;
    }

    public RabbitMqExchangeBuilder DurableExchange(bool durable = true)
    {
        Durable = durable;
        return this;
    }

    public RabbitMqExchangeBuilder AutoDeleteExchange(bool autoDelete = true)
    {
        AutoDelete = autoDelete;
        return this;
    }

    public RabbitMqExchangeBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    internal RabbitMqExchangeDefinition Build()
    {
        return new RabbitMqExchangeDefinition(
            Name,
            Type,
            DeclareMode,
            Durable,
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
