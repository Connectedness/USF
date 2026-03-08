using System;

namespace Usf.Core.Messaging.Errors;

public sealed class MessageTargetTypeMismatchException : Exception
{
    public MessageTargetTypeMismatchException(string targetName, Type actualMessageType, Type expectedMessageType)
        : base(
            $"Target '{targetName}' cannot publish messages of type '{actualMessageType}'. Expected '{expectedMessageType}'."
        )
    {
        TargetName = targetName;
        ActualMessageType = actualMessageType;
        ExpectedMessageType = expectedMessageType;
    }

    public string TargetName { get; }

    public Type ActualMessageType { get; }

    public Type ExpectedMessageType { get; }
}
