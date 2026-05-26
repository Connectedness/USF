using System;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class EmptyMessageTopology : IMessageTopology
{
    public Target GetRequiredTarget(Type messageType)
    {
        throw new MessageTargetNotFoundException(messageType);
    }

    public Target<T> GetRequiredTarget<T>()
    {
        throw new MessageTargetNotFoundException(typeof(T));
    }

    public bool TryGetTarget(Type messageType, out Target? target)
    {
        target = null;
        return false;
    }
}
