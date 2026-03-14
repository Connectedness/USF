using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqExchangeBindingBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    public RabbitMqExchangeBindingBuilder(
        string sourceExchangeName,
        string destinationExchangeName,
        string routingKey
    )
    {
        SourceExchangeName = RequireText(sourceExchangeName, nameof(sourceExchangeName));
        DestinationExchangeName = RequireText(destinationExchangeName, nameof(destinationExchangeName));
        RoutingKey = routingKey ?? string.Empty;
        DeclareMode = RabbitMqBindingDeclareMode.Ensure;
    }

    public string SourceExchangeName { get; }

    public string DestinationExchangeName { get; }

    public string RoutingKey { get; }

    public RabbitMqBindingDeclareMode DeclareMode { get; private set; }

    public RabbitMqExchangeBindingBuilder WithDeclareMode(RabbitMqBindingDeclareMode declareMode)
    {
        DeclareMode = declareMode;
        return this;
    }

    public RabbitMqExchangeBindingBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    internal RabbitMqExchangeBindingDefinition Build()
    {
        return new RabbitMqExchangeBindingDefinition(
            SourceExchangeName,
            DestinationExchangeName,
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
