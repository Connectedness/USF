using System;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public abstract class RabbitMqInboundEndpoint : InboundEndpoint
{
    protected RabbitMqInboundEndpoint(
        string name,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type serializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode,
        string queueName,
        Type inspectorType,
        RabbitMqInboundChannelGroup channelGroup
    )
        : base(
            name,
            "rabbitmq",
            topologyName,
            messageType,
            handlerType,
            serializerType,
            discriminator,
            handlerInvocation,
            ackMode
        )
    {
        QueueName = RequireText(queueName, nameof(queueName));
        InspectorType = inspectorType ?? throw new ArgumentNullException(nameof(inspectorType));
        ChannelGroup = channelGroup ?? throw new ArgumentNullException(nameof(channelGroup));

        if (!typeof(IInboundMessageInspector).IsAssignableFrom(InspectorType))
        {
            throw new ArgumentException(
                $"Inspector type '{InspectorType}' must implement '{typeof(IInboundMessageInspector)}'.",
                nameof(inspectorType)
            );
        }
    }

    public string QueueName { get; }

    public Type InspectorType { get; }

    public RabbitMqInboundChannelGroup ChannelGroup { get; }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}

public sealed class RabbitMqInboundEndpoint<TMessage> : RabbitMqInboundEndpoint
{
    public RabbitMqInboundEndpoint(
        string name,
        string topologyName,
        Type handlerType,
        Type serializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode,
        string queueName,
        Type inspectorType,
        RabbitMqInboundChannelGroup channelGroup
    )
        : base(
            name,
            topologyName,
            typeof(TMessage),
            handlerType,
            serializerType,
            discriminator,
            handlerInvocation,
            ackMode,
            queueName,
            inspectorType,
            channelGroup
        )
    {
        if (!typeof(IMessageHandler<TMessage>).IsAssignableFrom(handlerType))
        {
            throw new ArgumentException(
                $"Handler type '{handlerType}' must implement '{typeof(IMessageHandler<TMessage>)}'.",
                nameof(handlerType)
            );
        }
    }
}
