using System;

namespace Usf.Core.Messaging.Errors;

public sealed class MessageDeliveryException : Exception
{
    public MessageDeliveryException(
        string targetName,
        MessageDeliveryFailureReason reason,
        Exception? innerException = null
    )
        : base(CreateMessage(targetName, reason, innerException), innerException)
    {
        TargetName = targetName;
        Reason = reason;
    }

    public string TargetName { get; }

    public MessageDeliveryFailureReason Reason { get; }

    private static string CreateMessage(
        string targetName,
        MessageDeliveryFailureReason reason,
        Exception? innerException
    )
    {
        if (string.IsNullOrWhiteSpace(targetName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(targetName));
        }

        if (!Enum.IsDefined(typeof(MessageDeliveryFailureReason), reason))
        {
            throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unsupported delivery-failure reason.");
        }

        if (reason == MessageDeliveryFailureReason.Timeout && innerException is not null)
        {
            throw new ArgumentException(
                "A delivery timeout cannot provide an inner exception.",
                nameof(innerException)
            );
        }

        if (reason != MessageDeliveryFailureReason.Timeout && innerException is null)
        {
            throw new ArgumentException(
                "A delivery failure other than timeout must provide an inner exception.",
                nameof(innerException)
            );
        }

        return $"Delivery failed for outbound target '{targetName}' with reason '{reason}'.";
    }
}
