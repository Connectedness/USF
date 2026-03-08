using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

internal sealed class RabbitMqTarget<TMessage> : Target<TMessage>
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly string _exchangeName;
    private readonly bool _isMandatory;
    private readonly string _routingKey;

    public RabbitMqTarget(
        string name,
        IMessageSerializer serializer,
        RabbitMqConnectionManager connectionManager,
        string exchangeName,
        string routingKey,
        bool isMandatory
    )
        : base(name, "rabbitmq", serializer)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _exchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
        _routingKey = routingKey ?? string.Empty;
        _isMandatory = isMandatory;
    }

    protected override async Task DispatchAsync(
        TMessage message,
        SerializedMessage serializedMessage,
        CancellationToken cancellationToken
    )
    {
        var connection = await _connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var channel =
            await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        var properties = CreateBasicProperties(serializedMessage);
        await channel.BasicPublishAsync(
                exchange: _exchangeName,
                routingKey: _routingKey,
                mandatory: _isMandatory,
                basicProperties: properties,
                body: serializedMessage.Body,
                cancellationToken: cancellationToken
            )
           .ConfigureAwait(false);
    }

    private static BasicProperties CreateBasicProperties(SerializedMessage serializedMessage)
    {
        BasicProperties properties = new ()
        {
            ContentType = serializedMessage.ContentType,
            ContentEncoding = serializedMessage.ContentEncoding,
            MessageId = serializedMessage.MessageId,
            CorrelationId = serializedMessage.CorrelationId
        };

        if (serializedMessage.Headers.Count > 0)
        {
            Dictionary<string, object?> headers = new (serializedMessage.Headers.Count, StringComparer.Ordinal);

            foreach (var header in serializedMessage.Headers)
            {
                headers[header.Key] = header.Value;
            }

            properties.Headers = headers;
        }

        return properties;
    }
}
