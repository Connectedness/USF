using System;

namespace Usf.Core.Messaging.Errors;

public sealed class MessageSerializationException : Exception
{
    public MessageSerializationException(Type messageType, Exception innerException)
        : base($"Serialization failed for message type '{messageType}'.", innerException)
    {
        MessageType = messageType;
    }

    public Type MessageType { get; }
}
