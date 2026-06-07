using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

/// <summary>
/// The immutable Core topology aggregate. It stores outbound targets indexed by name and by associated message
/// type, and inbound endpoint definitions indexed by name. Outbound targets and inbound endpoints stay
/// independent entry types: the topology can list and resolve either, but publishing code only selects outbound
/// targets while the inbound runtime owns inbound endpoints.
/// </summary>
public sealed class Topology : ITopology
{
    private readonly IReadOnlyDictionary<string, InboundEndpoint> _endpointsByName;
    private readonly IReadOnlyDictionary<Type, OutboundTarget> _targetsByMessageType;
    private readonly IReadOnlyDictionary<string, OutboundTarget> _targetsByName;

    public Topology(
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
        _targetsByMessageType = new ReadOnlyDictionary<Type, OutboundTarget>(
            new Dictionary<Type, OutboundTarget>(targetsByMessageType)
        );
        _targetsByName = new ReadOnlyDictionary<string, OutboundTarget>(
            new Dictionary<string, OutboundTarget>(targetsByName, StringComparer.Ordinal)
        );
        _endpointsByName = new ReadOnlyDictionary<string, InboundEndpoint>(
            new Dictionary<string, InboundEndpoint>(endpointsByName, StringComparer.Ordinal)
        );
        OutboundTargets = _targetsByMessageType.Values.Concat(_targetsByName.Values).Distinct().ToArray();
        InboundEndpoints = _endpointsByName.Values.ToArray();
    }

    public TopologyName TopologyName { get; }

    public IReadOnlyCollection<OutboundTarget> OutboundTargets { get; }

    public IReadOnlyCollection<InboundEndpoint> InboundEndpoints { get; }

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
