using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqBindingBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    public RabbitMqBindingBuilder(string exchangeName, string queueName, string routingKey)
    {
        ExchangeName = RequireText(exchangeName, nameof(exchangeName));
        QueueName = RequireText(queueName, nameof(queueName));
        RoutingKey = routingKey ?? string.Empty;
        DeclareMode = RabbitMqDeclareMode.Active;
    }

    public string ExchangeName { get; }

    public string QueueName { get; }

    public string RoutingKey { get; }

    public RabbitMqDeclareMode DeclareMode { get; private set; }

    public RabbitMqBindingBuilder WithDeclareMode(RabbitMqDeclareMode declareMode)
    {
        DeclareMode = declareMode;
        return this;
    }

    public RabbitMqBindingBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    internal RabbitMqBindingDefinition Build()
    {
        return new RabbitMqBindingDefinition(
            ExchangeName,
            QueueName,
            RoutingKey,
            DeclareMode,
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
