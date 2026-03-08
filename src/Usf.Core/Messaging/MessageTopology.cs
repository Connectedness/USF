using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class MessageTopology : IMessageTopology, ITargetRegistry
{
    private readonly IReadOnlyDictionary<Type, Target> _targetsByMessageType;
    private readonly IReadOnlyDictionary<string, Target> _targetsByName;

    public MessageTopology(IDictionary<Type, Target> targetsByMessageType, IDictionary<string, Target> targetsByName)
    {
        if (targetsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(targetsByMessageType));
        }

        if (targetsByName is null)
        {
            throw new ArgumentNullException(nameof(targetsByName));
        }

        _targetsByMessageType =
            new ReadOnlyDictionary<Type, Target>(new Dictionary<Type, Target>(targetsByMessageType));
        _targetsByName =
            new ReadOnlyDictionary<string, Target>(
                new Dictionary<string, Target>(targetsByName, StringComparer.Ordinal)
            );
    }

    public Target GetRequiredTarget(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        if (!_targetsByMessageType.TryGetValue(messageType, out var target))
        {
            throw new MessageTargetNotFoundException(messageType);
        }

        return target;
    }

    public Target<T> GetRequiredTarget<T>()
    {
        var target = GetRequiredTarget(typeof(T));

        if (target is not Target<T> typedTarget)
        {
            throw new MessageTargetTypeMismatchException(target.Name, typeof(T), target.MessageType);
        }

        return typedTarget;
    }

    public bool TryGetTarget(Type messageType, out Target? target)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _targetsByMessageType.TryGetValue(messageType, out target);
    }

    public Target GetRequiredTarget(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_targetsByName.TryGetValue(name, out var target))
        {
            throw new MessageTargetNotFoundException(name);
        }

        return target;
    }

    public Target<T> GetRequiredTarget<T>(string name)
    {
        var target = GetRequiredTarget(name);

        if (target is not Target<T> typedTarget)
        {
            throw new MessageTargetTypeMismatchException(target.Name, typeof(T), target.MessageType);
        }

        return typedTarget;
    }

    public bool TryGetTarget(string name, out Target? target)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        return _targetsByName.TryGetValue(name, out target);
    }
}
