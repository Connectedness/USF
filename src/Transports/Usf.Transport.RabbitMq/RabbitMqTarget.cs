using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

internal abstract class RabbitMqTarget<TMessage> : Target<TMessage>
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly string _exchangeName;
    private readonly bool _isMandatory;

    protected RabbitMqTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        bool isMandatory
    )
        : base(name, "rabbitmq", serializer)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _exchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
        _isMandatory = isMandatory;
    }

    protected sealed override async Task DispatchAsync(
        TMessage message,
        SerializedMessage serializedMessage,
        CancellationToken cancellationToken
    )
    {
        var connection = await _connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var channel =
            await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        var properties = CreateBasicProperties(serializedMessage, GetRouteHeaders(message));
        await channel.BasicPublishAsync(
                exchange: _exchangeName,
                routingKey: GetRoutingKey(message),
                mandatory: _isMandatory,
                basicProperties: properties,
                body: serializedMessage.Body,
                cancellationToken: cancellationToken
            )
           .ConfigureAwait(false);
    }

    protected virtual string GetRoutingKey(TMessage message)
    {
        return string.Empty;
    }

    protected virtual IReadOnlyDictionary<string, object?> GetRouteHeaders(TMessage message)
    {
        return EmptyHeaders.Instance;
    }

    private static BasicProperties CreateBasicProperties(
        SerializedMessage serializedMessage,
        IReadOnlyDictionary<string, object?> routeHeaders
    )
    {
        BasicProperties properties = new ()
        {
            ContentType = serializedMessage.ContentType,
            ContentEncoding = serializedMessage.ContentEncoding,
            MessageId = serializedMessage.MessageId,
            CorrelationId = serializedMessage.CorrelationId
        };

        if (routeHeaders.Count == 0 && serializedMessage.Headers.Count == 0)
        {
            return properties;
        }

        Dictionary<string, object?> headers = new (
            routeHeaders.Count + serializedMessage.Headers.Count,
            StringComparer.Ordinal
        );

        foreach (var header in routeHeaders)
        {
            headers[header.Key] = header.Value;
        }

        foreach (var header in serializedMessage.Headers)
        {
            headers[header.Key] = header.Value;
        }

        properties.Headers = headers;
        return properties;
    }

    private static class EmptyHeaders
    {
        public static readonly IReadOnlyDictionary<string, object?> Instance =
            new Dictionary<string, object?>(0, StringComparer.Ordinal);
    }
}

internal sealed class RabbitMqFanoutTarget<TMessage> : RabbitMqTarget<TMessage>
{
    public RabbitMqFanoutTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        bool isMandatory
    )
        : base(name, serializer, connectionManager, exchangeName, isMandatory) { }
}

internal abstract class RabbitMqRoutingKeyTarget<TMessage> : RabbitMqTarget<TMessage>
{
    private readonly Func<TMessage, string> _routingKeyFactory;

    protected RabbitMqRoutingKeyTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        bool isMandatory,
        Func<TMessage, string> routingKeyFactory
    )
        : base(name, serializer, connectionManager, exchangeName, isMandatory)
    {
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
    }

    protected override string GetRoutingKey(TMessage message)
    {
        return _routingKeyFactory(message) ??
               throw new InvalidOperationException("The RabbitMQ routing key factory returned null.");
    }
}

internal sealed class RabbitMqDirectTarget<TMessage> : RabbitMqRoutingKeyTarget<TMessage>
{
    public RabbitMqDirectTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        bool isMandatory,
        Func<TMessage, string> routingKeyFactory
    )
        : base(name, serializer, connectionManager, exchangeName, isMandatory, routingKeyFactory) { }
}

internal sealed class RabbitMqTopicTarget<TMessage> : RabbitMqRoutingKeyTarget<TMessage>
{
    public RabbitMqTopicTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        bool isMandatory,
        Func<TMessage, string> routingKeyFactory
    )
        : base(name, serializer, connectionManager, exchangeName, isMandatory, routingKeyFactory) { }
}

internal sealed class RabbitMqHeadersTarget<TMessage> : RabbitMqTarget<TMessage>
{
    private readonly IReadOnlyDictionary<string, object?> _headers;

    public RabbitMqHeadersTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        bool isMandatory,
        IReadOnlyDictionary<string, object?> headers
    )
        : base(name, serializer, connectionManager, exchangeName, isMandatory)
    {
        _headers = headers ?? throw new ArgumentNullException(nameof(headers));
    }

    protected override IReadOnlyDictionary<string, object?> GetRouteHeaders(TMessage message)
    {
        return _headers;
    }
}
