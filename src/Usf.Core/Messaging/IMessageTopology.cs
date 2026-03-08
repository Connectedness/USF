using System;

namespace Usf.Core.Messaging;

public interface IMessageTopology
{
    Target GetRequiredTarget(Type messageType);

    Target<T> GetRequiredTarget<T>();

    bool TryGetTarget(Type messageType, out Target? target);
}
