using System;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqPublishRouteBuilder<TMessage>
{
    private string? _exchangeName;
    private string _routingKey = string.Empty;
    private Type? _serializerType;
    private string? _targetName;

    public bool IsMandatory { get; private set; }

    public RabbitMqPublishRouteBuilder<TMessage> ToExchange(string exchangeName, string routingKey = "")
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _routingKey = routingKey ?? string.Empty;
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        _serializerType = typeof(TSerializer);
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> WithTargetName(string targetName)
    {
        _targetName = RequireText(targetName, nameof(targetName));
        return this;
    }

    public RabbitMqPublishRouteBuilder<TMessage> Mandatory(bool mandatory = true)
    {
        IsMandatory = mandatory;
        return this;
    }

    internal RabbitMqPublishRouteConfiguration Build()
    {
        return new RabbitMqPublishRouteConfiguration(
            typeof(TMessage),
            _exchangeName,
            _routingKey,
            _targetName,
            _serializerType,
            IsMandatory
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
