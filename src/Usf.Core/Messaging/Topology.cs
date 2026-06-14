using System;
using System.Collections.Frozen;
using System.Collections.Immutable;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

/// <summary>
/// The immutable base for a compiled transport topology. It stores outbound targets indexed by name and message
/// type, and inbound endpoints indexed by name.
/// </summary>
public abstract class Topology
{
    public const string DefaultName = "default";

    private readonly FrozenDictionary<string, InboundEndpoint> _endpointsByName;
    private readonly FrozenDictionary<Type, OutboundTarget> _targetsByMessageType;
    private readonly FrozenDictionary<string, OutboundTarget> _targetsByName;

    protected Topology(string name, TopologyData data)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        Name = name;
        _targetsByMessageType = data.TargetsByMessageType;
        _targetsByName = data.TargetsByName;
        _endpointsByName = data.EndpointsByName;
        OutboundTargets = data.OutboundTargets;
        InboundEndpoints = data.InboundEndpoints;
    }

    public string Name { get; }

    public ImmutableArray<OutboundTarget> OutboundTargets { get; }

    public ImmutableArray<InboundEndpoint> InboundEndpoints { get; }

    public bool IsEmpty => OutboundTargets.IsDefaultOrEmpty && InboundEndpoints.IsDefaultOrEmpty;

    public bool IsOutboundOnly => !OutboundTargets.IsDefaultOrEmpty && InboundEndpoints.IsDefaultOrEmpty;

    public bool IsInboundOnly => OutboundTargets.IsDefaultOrEmpty && !InboundEndpoints.IsDefaultOrEmpty;

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
