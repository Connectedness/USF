using System;

namespace Usf.Core.Messaging;

public abstract class InboundEndpoint
{
    protected InboundEndpoint(
        string name,
        string transportName,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type serializerType,
        string discriminator,
        MessageAckMode ackMode
    )
    {
        Name = RequireText(name, nameof(name));
        TransportName = RequireText(transportName, nameof(transportName));
        TopologyName = RequireText(topologyName, nameof(topologyName));
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        HandlerType = handlerType ?? throw new ArgumentNullException(nameof(handlerType));
        SerializerType = serializerType ?? throw new ArgumentNullException(nameof(serializerType));
        Discriminator = RequireText(discriminator, nameof(discriminator));

        if (!typeof(IMessageSerializer).IsAssignableFrom(SerializerType))
        {
            throw new ArgumentException(
                $"Serializer type '{SerializerType}' must implement '{typeof(IMessageSerializer)}'.",
                nameof(serializerType)
            );
        }

        if (!Enum.IsDefined(typeof(MessageAckMode), ackMode))
        {
            throw new ArgumentOutOfRangeException(nameof(ackMode), ackMode, "Unsupported acknowledgement mode.");
        }

        AckMode = ackMode;
    }

    public string Name { get; }

    public string TransportName { get; }

    public string TopologyName { get; }

    public Type MessageType { get; }

    public Type HandlerType { get; }

    public Type SerializerType { get; }

    public string Discriminator { get; }

    public MessageAckMode AckMode { get; }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}

public class InboundEndpoint<TMessage> : InboundEndpoint
{
    public InboundEndpoint(
        string name,
        string transportName,
        string topologyName,
        Type handlerType,
        Type serializerType,
        string discriminator,
        MessageAckMode ackMode = MessageAckMode.Auto
    )
        : base(
            name,
            transportName,
            topologyName,
            typeof(TMessage),
            handlerType,
            serializerType,
            discriminator,
            ackMode
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
