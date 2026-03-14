using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqPublishRouteBuilder<TMessage>
{
    private readonly Dictionary<string, object?> _headers = new (StringComparer.Ordinal);
    private string? _exchangeName;
    private string? _routingKey;
    private Func<TMessage, string>? _routingKeyFactory;
    private RabbitMqPublishRouteScenario _scenario;
    private Type? _serializerType;

    public bool IsMandatory { get; private set; }

    public RabbitMqPublishRouteBuilder<TMessage> ToFanoutExchange(string exchangeName)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqPublishRouteScenario.Fanout;
        _routingKey = null;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> ToDirectExchange(string exchangeName, string routingKey)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqPublishRouteScenario.Direct;
        _routingKey = routingKey ?? string.Empty;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> ToDirectExchange(
        string exchangeName,
        Func<TMessage, string> routingKeyFactory
    )
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqPublishRouteScenario.Direct;
        _routingKey = null;
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
        _headers.Clear();
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> ToTopicExchange(string exchangeName, string routingKey)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqPublishRouteScenario.Topic;
        _routingKey = routingKey ?? string.Empty;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> ToTopicExchange(
        string exchangeName,
        Func<TMessage, string> routingKeyFactory
    )
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqPublishRouteScenario.Topic;
        _routingKey = null;
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
        _headers.Clear();
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> ToHeadersExchange(string exchangeName)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqPublishRouteScenario.Headers;
        _routingKey = null;
        _routingKeyFactory = null;
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> WithHeader(string name, object? value)
    {
        _headers[RequireText(name, nameof(name))] = value;
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        _serializerType = typeof(TSerializer);
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> Mandatory(bool mandatory = true)
    {
        IsMandatory = mandatory;
        return this;
    }

    internal RabbitMqPublishRouteConfiguration Build(string? targetName)
    {
        if (_exchangeName is null)
        {
            throw new InvalidOperationException("A RabbitMQ publish route must select an exchange type.");
        }

        return _scenario switch
        {
            RabbitMqPublishRouteScenario.Fanout => new RabbitMqFanoutPublishRouteConfiguration(
                typeof(TMessage),
                _exchangeName,
                targetName,
                _serializerType,
                IsMandatory
            ),
            RabbitMqPublishRouteScenario.Direct => new RabbitMqDirectPublishRouteConfiguration(
                typeof(TMessage),
                _exchangeName,
                targetName,
                _serializerType,
                IsMandatory,
                _routingKey,
                _routingKeyFactory
            ),
            RabbitMqPublishRouteScenario.Topic => new RabbitMqTopicPublishRouteConfiguration(
                typeof(TMessage),
                _exchangeName,
                targetName,
                _serializerType,
                IsMandatory,
                _routingKey,
                _routingKeyFactory
            ),
            RabbitMqPublishRouteScenario.Headers => new RabbitMqHeadersPublishRouteConfiguration(
                typeof(TMessage),
                _exchangeName,
                targetName,
                _serializerType,
                IsMandatory,
                new ReadOnlyDictionary<string, object?>(_headers)
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(_scenario), _scenario, "Unsupported route scenario.")
        };
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

internal enum RabbitMqPublishRouteScenario
{
    Fanout = 0,
    Direct = 1,
    Topic = 2,
    Headers = 3
}
