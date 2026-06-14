using System;

namespace Usf.Core.Messaging.Errors;

public sealed class MessageDeserializationException : Exception
{
    public MessageDeserializationException(Type messageType, Exception innerException)
        : base($"Deserialization failed for message type '{messageType}'.", innerException)
    {
        MessageType = messageType;
    }

    public Type MessageType { get; }
}
