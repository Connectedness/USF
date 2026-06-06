using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class OutboundTopology : Topology<OutboundTarget>, IOutboundTopology
{
    private readonly IReadOnlyDictionary<Type, OutboundTarget> _targetsByMessageType;

    public OutboundTopology(
        IDictionary<Type, OutboundTarget> targetsByMessageType,
        IDictionary<string, OutboundTarget> targetsByName
    ) : base(targetsByName)
    {
        if (targetsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(targetsByMessageType));
        }

        _targetsByMessageType =
            new ReadOnlyDictionary<Type, OutboundTarget>(
                new Dictionary<Type, OutboundTarget>(targetsByMessageType)
            );
        Targets = _targetsByMessageType.Values.Concat(Entries).Distinct().ToArray();
    }

    public IReadOnlyCollection<OutboundTarget> Targets { get; }

    public OutboundTarget GetRequiredTarget(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        if (!_targetsByMessageType.TryGetValue(messageType, out var target))
        {
            throw new OutboundTargetNotFoundException(messageType);
        }

        return target;
    }

    public OutboundTarget<T> GetRequiredTarget<T>()
    {
        var target = GetRequiredTarget(typeof(T));

        if (target is not OutboundTarget<T> typedTarget)
        {
            throw new OutboundTargetTypeMismatchException(target.Name, typeof(T), target.MessageType);
        }

        return typedTarget;
    }

    public bool TryGetTarget(Type messageType, out OutboundTarget? target)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _targetsByMessageType.TryGetValue(messageType, out target);
    }

    public OutboundTarget GetRequiredTarget(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!TryGetEntry(name, out var target) || target is null)
        {
            throw new OutboundTargetNotFoundException(name);
        }

        return target;
    }

    public OutboundTarget<T> GetRequiredTarget<T>(string name)
    {
        var target = GetRequiredTarget(name);

        if (target is not OutboundTarget<T> typedTarget)
        {
            throw new OutboundTargetTypeMismatchException(target.Name, typeof(T), target.MessageType);
        }

        return typedTarget;
    }

    public bool TryGetTarget(string name, out OutboundTarget? target)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        return TryGetEntry(name, out target);
    }
}
