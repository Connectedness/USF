using System;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public abstract class InboundEndpoint
{
    private readonly MessageDelegate _handlerInvocation;

    protected InboundEndpoint(
        string name,
        string transportName,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type serializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
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
        _handlerInvocation = handlerInvocation ?? throw new ArgumentNullException(nameof(handlerInvocation));

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

    public Task InvokeHandlerAsync(IncomingMessageContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (context.Message is null)
        {
            throw new InvalidOperationException("The inbound message has not been deserialized.");
        }

        return _handlerInvocation(context);
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

public class InboundEndpoint<TMessage> : InboundEndpoint
{
    public InboundEndpoint(
        string name,
        string transportName,
        string topologyName,
        Type handlerType,
        Type serializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
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
            handlerInvocation,
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
