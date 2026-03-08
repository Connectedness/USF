using System;

namespace Usf.Core.Messaging.Errors;

public sealed class MessageTargetNotFoundException : Exception
{
    public MessageTargetNotFoundException(Type messageType)
        : base($"No target is registered for message type '{messageType}'.")
    {
        MessageType = messageType;
    }

    public MessageTargetNotFoundException(string targetName)
        : base($"No target is registered with name '{targetName}'.")
    {
        TargetName = targetName;
    }

    public Type? MessageType { get; }

    public string? TargetName { get; }
}
