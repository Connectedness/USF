using System;
using System.Collections.Generic;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundEndpointBuilder
{
    private readonly List<RabbitMqInboundHandlerDefinition> _handlers = [];
    private readonly string _queueName;
    private MessageAckMode _ackMode = MessageAckMode.Auto;
    private int _channelCount = 1;
    private string? _channelGroupName;
    private ushort _consumerDispatchConcurrency = 1;
    private Type _inspectorType = typeof(CloudEventsInboundMessageInspector);
    private ushort _prefetchCount = 1;
    private Type _serializerType = typeof(CloudEventMessageSerializer);

    public RabbitMqInboundEndpointBuilder(string queueName)
    {
        _queueName = RequireText(queueName, nameof(queueName));
    }

    public RabbitMqInboundEndpointBuilder PrefetchCount(ushort prefetchCount)
    {
        if (prefetchCount == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prefetchCount),
                prefetchCount,
                "The value must be greater than zero."
            );
        }

        _prefetchCount = prefetchCount;
        return this;
    }

    public RabbitMqInboundEndpointBuilder Concurrency(ushort consumerDispatchConcurrency)
    {
        if (consumerDispatchConcurrency == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerDispatchConcurrency),
                consumerDispatchConcurrency,
                "The value must be greater than zero."
            );
        }

        _consumerDispatchConcurrency = consumerDispatchConcurrency;
        return this;
    }

    public RabbitMqInboundEndpointBuilder ChannelCount(int channelCount)
    {
        if (channelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(channelCount),
                channelCount,
                "The value must be greater than zero."
            );
        }

        _channelCount = channelCount;
        return this;
    }

    public RabbitMqInboundEndpointBuilder UseChannelGroup(string channelGroupName)
    {
        _channelGroupName = RequireText(channelGroupName, nameof(channelGroupName));
        return this;
    }

    public RabbitMqInboundEndpointBuilder UseInspector<TInspector>()
        where TInspector : class, IInboundMessageInspector
    {
        _inspectorType = typeof(TInspector);
        return this;
    }

    public RabbitMqInboundEndpointBuilder WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        _serializerType = typeof(TSerializer);
        return this;
    }

    public RabbitMqInboundEndpointBuilder WithAckMode(MessageAckMode ackMode)
    {
        if (!Enum.IsDefined(typeof(MessageAckMode), ackMode))
        {
            throw new ArgumentOutOfRangeException(nameof(ackMode), ackMode, "Unsupported acknowledgement mode.");
        }

        _ackMode = ackMode;
        return this;
    }

    public RabbitMqInboundEndpointBuilder ManualAck()
    {
        return WithAckMode(MessageAckMode.Manual);
    }

    /// <summary>
    /// Adds a handler for <typeparamref name="TMessage" />. The concrete <typeparamref name="THandler" /> type is
    /// auto-registered as scoped and resolved from the per-delivery scope. Register the concrete handler type before
    /// calling <c>AddRabbitMq*Topology</c> to choose a different lifetime; auto-registration yields to an existing
    /// registration.
    /// </summary>
    public RabbitMqInboundEndpointBuilder Handle<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        return HandleNamed<TMessage, THandler>(endpointName: null);
    }

    /// <summary>
    /// Adds a named handler for <typeparamref name="TMessage" />. The concrete <typeparamref name="THandler" /> type
    /// is auto-registered as scoped and resolved from the per-delivery scope. Register the concrete handler type
    /// before calling <c>AddRabbitMq*Topology</c> to choose a different lifetime; auto-registration yields to an
    /// existing registration.
    /// </summary>
    public RabbitMqInboundEndpointBuilder HandleNamed<TMessage, THandler>(string? endpointName)
        where THandler : class, IMessageHandler<TMessage>
    {
        if (typeof(THandler).IsInterface || typeof(THandler).IsAbstract)
        {
            throw new ArgumentException(
                $"Handler type '{typeof(THandler)}' must be a concrete class.",
                nameof(THandler)
            );
        }

        _handlers.Add(
            new RabbitMqInboundHandlerDefinition(
                _queueName,
                endpointName,
                typeof(TMessage),
                typeof(THandler),
                MessageHandlerInvocation.Create<TMessage, THandler>(),
                _serializerType,
                _inspectorType,
                _channelGroupName,
                _channelCount,
                _prefetchCount,
                _consumerDispatchConcurrency,
                _ackMode
            )
        );
        return this;
    }

    internal IReadOnlyList<RabbitMqInboundHandlerDefinition> Build()
    {
        return _handlers.AsReadOnly();
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
