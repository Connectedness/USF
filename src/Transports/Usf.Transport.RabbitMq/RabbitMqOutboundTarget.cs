using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Usf.Abstractions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public abstract class RabbitMqOutboundTarget<TMessage> : OutboundTarget<TMessage>
{
    /// <summary>
    /// Prefixes CloudEvents attributes carried as AMQP application headers by the AMQP protocol binding.
    /// </summary>
    public const string CloudEventsHeaderPrefix = "cloudEvents:";

    private readonly RabbitMqChannelGroup _channelGroup;
    private readonly string _exchangeName;
    private readonly bool _isMandatory;

    protected RabbitMqOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory
    )
        : base(name, "rabbitmq", serializer, messageContractRegistry, topologyName)
    {
        _channelGroup = channelGroup ?? throw new ArgumentNullException(nameof(channelGroup));
        _exchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
        _isMandatory = isMandatory;
    }

    public sealed override Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    )
    {
        return DispatchAsync(
            message,
            GetRawRoutingKey(),
            GetRawRouteHeaders(),
            cancellationToken
        );
    }

    protected sealed override Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        return DispatchAsync(
            envelope,
            ResolveRoutingKey(message, routingKey),
            GetRouteHeaders(message),
            cancellationToken
        );
    }

    protected virtual string GetRawRoutingKey()
    {
        return string.Empty;
    }

    protected virtual IReadOnlyDictionary<string, object?> GetRawRouteHeaders()
    {
        return EmptyHeaders.Instance;
    }

    protected virtual string GetRoutingKey(TMessage message)
    {
        return string.Empty;
    }

    protected virtual string ResolveRoutingKey(TMessage message, string? routingKey)
    {
        return string.IsNullOrWhiteSpace(routingKey) ? GetRoutingKey(message) : routingKey!;
    }

    protected virtual IReadOnlyDictionary<string, object?> GetRouteHeaders(TMessage message)
    {
        return EmptyHeaders.Instance;
    }

    private async Task DispatchAsync(
        SerializedMessage serializedMessage,
        string routingKey,
        IReadOnlyDictionary<string, object?> routeHeaders,
        CancellationToken cancellationToken
    )
    {
        await DispatchAsync(
                serializedMessage.Body,
                CreateBasicProperties(serializedMessage, routeHeaders),
                routingKey,
                cancellationToken
            )
           .ConfigureAwait(false);
    }

    private async Task DispatchAsync(
        CloudEventEnvelope envelope,
        string routingKey,
        IReadOnlyDictionary<string, object?> routeHeaders,
        CancellationToken cancellationToken
    )
    {
        await DispatchAsync(
                envelope.Data,
                CreateBasicProperties(envelope, routeHeaders),
                routingKey,
                cancellationToken
            )
           .ConfigureAwait(false);
    }

    private async Task DispatchAsync(
        ReadOnlyMemory<byte> body,
        BasicProperties properties,
        string routingKey,
        CancellationToken cancellationToken
    )
    {
        await using var lease = await _channelGroup.AcquireAsync(cancellationToken).ConfigureAwait(false);

        if (_channelGroup.PublisherConfirmMode == RabbitMqPublisherConfirmMode.FireAndForget)
        {
            await PublishAsync(lease.Channel, properties, body, routingKey, cancellationToken)
               .ConfigureAwait(false);
            return;
        }

        using var timeoutCancellationTokenSource =
            new CancellationTokenSource(_channelGroup.PublisherConfirmTimeout);
        using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCancellationTokenSource.Token
        );

        try
        {
            await PublishAsync(
                    lease.Channel,
                    properties,
                    body,
                    routingKey,
                    linkedCancellationTokenSource.Token
                )
               .ConfigureAwait(false);
        }
        catch (PublishException exception)
        {
            var reason = exception.IsReturn ?
                MessageDeliveryFailureReason.Returned :
                MessageDeliveryFailureReason.Nacked;
            throw new MessageDeliveryException(Name, reason, exception);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested &&
                                                 timeoutCancellationTokenSource.IsCancellationRequested)
        {
            throw new MessageDeliveryException(Name, MessageDeliveryFailureReason.Timeout);
        }
    }

    private ValueTask PublishAsync(
        IChannel channel,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        string routingKey,
        CancellationToken cancellationToken
    )
    {
        return channel.BasicPublishAsync(
            exchange: _exchangeName,
            routingKey: routingKey,
            mandatory: _isMandatory,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken
        );
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

    /// <summary>
    /// Applies the CloudEvents v1.0 AMQP binary content-mode binding over AMQP 0.9.1.
    /// </summary>
    private static BasicProperties CreateBasicProperties(
        CloudEventEnvelope envelope,
        IReadOnlyDictionary<string, object?> routeHeaders
    )
    {
        BasicProperties properties = new ()
        {
            ContentType = envelope.DataContentType,
            MessageId = envelope.Id
        };

        var extensionCount = envelope.Extensions?.Count ?? 0;
        Dictionary<string, object?> headers = new (
            routeHeaders.Count + extensionCount + 10, // +10 adjusted for tracing
            StringComparer.Ordinal
        );

        foreach (var header in routeHeaders)
        {
            headers[header.Key] = header.Value;
        }

        if (envelope.Extensions is not null)
        {
            foreach (var extension in envelope.Extensions)
            {
                headers[GetCloudEventsHeaderName(extension.Key)] = extension.Value;
            }
        }

        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Id)] = envelope.Id;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.SpecVersion)] = envelope.SpecVersion;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Source)] = envelope.Source;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Type)] = envelope.Type;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Time)] = envelope.Time.ToString("O");

        if (envelope.Subject is not null)
        {
            headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Subject)] = envelope.Subject;
        }

        if (envelope.DataSchema is not null)
        {
            headers[GetCloudEventsHeaderName(CloudEventAttributeNames.DataSchema)] = envelope.DataSchema;
        }

        TraceContextHeaders.Inject(headers);

        properties.Headers = headers;
        return properties;
    }

    private static string GetCloudEventsHeaderName(string attributeName)
    {
        return $"{CloudEventsHeaderPrefix}{attributeName}";
    }

    private static class EmptyHeaders
    {
        public static readonly IReadOnlyDictionary<string, object?> Instance =
            new Dictionary<string, object?>(0, StringComparer.Ordinal);
    }
}

public sealed class RabbitMqFanoutOutboundTarget<TMessage> : RabbitMqOutboundTarget<TMessage>
{
    public RabbitMqFanoutOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory
    )
        : base(name, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName, isMandatory) { }
}

public abstract class RabbitMqRoutingKeyOutboundTarget<TMessage> : RabbitMqOutboundTarget<TMessage>
{
    private readonly string? _constantRoutingKey;
    private readonly Func<TMessage, string>? _routingKeyFactory;

    protected RabbitMqRoutingKeyOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        string? constantRoutingKey,
        Func<TMessage, string>? routingKeyFactory
    )
        : base(name, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName, isMandatory)
    {
        if (constantRoutingKey is null && routingKeyFactory is null)
        {
            throw new ArgumentException("A routing-key target must provide a constant key or a key factory.");
        }

        if (constantRoutingKey is not null && routingKeyFactory is not null)
        {
            throw new ArgumentException("A routing-key target cannot provide both a constant key and a key factory.");
        }

        _constantRoutingKey = constantRoutingKey;
        _routingKeyFactory = routingKeyFactory;
    }

    protected override string GetRawRoutingKey()
    {
        return _constantRoutingKey ??
               throw new InvalidOperationException(
                   "Raw publishing is not supported for RabbitMQ outbound targets with message-derived routing keys."
               );
    }

    protected override string GetRoutingKey(TMessage message)
    {
        if (_constantRoutingKey is not null)
        {
            return _constantRoutingKey;
        }

        return _routingKeyFactory!(message) ??
               throw new InvalidOperationException("The RabbitMQ routing key factory returned null.");
    }
}

public sealed class RabbitMqDirectOutboundTarget<TMessage> : RabbitMqRoutingKeyOutboundTarget<TMessage>
{
    public RabbitMqDirectOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        string? constantRoutingKey,
        Func<TMessage, string>? routingKeyFactory
    )
        : base(
            name,
            serializer,
            messageContractRegistry,
            topologyName,
            channelGroup,
            exchangeName,
            isMandatory,
            constantRoutingKey,
            routingKeyFactory
        ) { }
}

public sealed class RabbitMqTopicOutboundTarget<TMessage> : RabbitMqRoutingKeyOutboundTarget<TMessage>
{
    public RabbitMqTopicOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        string? constantRoutingKey,
        Func<TMessage, string>? routingKeyFactory
    )
        : base(
            name,
            serializer,
            messageContractRegistry,
            topologyName,
            channelGroup,
            exchangeName,
            isMandatory,
            constantRoutingKey,
            routingKeyFactory
        ) { }
}

public sealed class RabbitMqHeadersOutboundTarget<TMessage> : RabbitMqOutboundTarget<TMessage>
{
    private readonly IReadOnlyDictionary<string, object?> _headers;

    public RabbitMqHeadersOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        IReadOnlyDictionary<string, object?> headers
    )
        : base(name, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName, isMandatory)
    {
        _headers = headers ?? throw new ArgumentNullException(nameof(headers));
    }

    protected override IReadOnlyDictionary<string, object?> GetRawRouteHeaders()
    {
        return _headers;
    }

    protected override IReadOnlyDictionary<string, object?> GetRouteHeaders(TMessage message)
    {
        return _headers;
    }
}
