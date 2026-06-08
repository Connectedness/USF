using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

/// <summary>
/// The immutable Core topology definition. It stores outbound targets indexed by name and by associated message
/// type, and inbound endpoint definitions indexed by name. Outbound targets and inbound endpoints stay
/// independent entry types: the definition can list and resolve either, but publishing code only selects outbound
/// targets while the inbound runtime owns inbound endpoints. A definition is built once during topology
/// compilation and read on every publish and dispatch, so its lookups are backed by <see cref="FrozenDictionary{TKey,TValue}" />.
/// </summary>
public sealed class TopologyDefinition
{
    private readonly FrozenDictionary<string, InboundEndpoint> _endpointsByName;
    private readonly FrozenDictionary<Type, OutboundTarget> _targetsByMessageType;
    private readonly FrozenDictionary<string, OutboundTarget> _targetsByName;

    public TopologyDefinition(
        TopologyName topologyName,
        IDictionary<Type, OutboundTarget> targetsByMessageType,
        IDictionary<string, OutboundTarget> targetsByName,
        IDictionary<string, InboundEndpoint> endpointsByName
    )
    {
        if (targetsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(targetsByMessageType));
        }

        if (targetsByName is null)
        {
            throw new ArgumentNullException(nameof(targetsByName));
        }

        if (endpointsByName is null)
        {
            throw new ArgumentNullException(nameof(endpointsByName));
        }

        TopologyName = topologyName;
        _targetsByMessageType = targetsByMessageType.ToFrozenDictionary();
        _targetsByName = targetsByName.ToFrozenDictionary(StringComparer.Ordinal);
        _endpointsByName = endpointsByName.ToFrozenDictionary(StringComparer.Ordinal);
        OutboundTargets = [.._targetsByMessageType.Values.Concat(_targetsByName.Values).Distinct()];
        InboundEndpoints = [.._endpointsByName.Values];
    }

    public TopologyName TopologyName { get; }

    public ImmutableArray<OutboundTarget> OutboundTargets { get; }

    public ImmutableArray<InboundEndpoint> InboundEndpoints { get; }

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

        if (!_targetsByName.TryGetValue(name, out var target))
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

        return _targetsByName.TryGetValue(name, out target);
    }

    public InboundEndpoint GetRequiredEndpoint(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_endpointsByName.TryGetValue(name, out var endpoint))
        {
            throw new InboundEndpointNotFoundException(name);
        }

        return endpoint;
    }

    public bool TryGetEndpoint(string name, out InboundEndpoint? endpoint)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        return _endpointsByName.TryGetValue(name, out endpoint);
    }
}
