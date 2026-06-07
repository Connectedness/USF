using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

/// <summary>
/// Describes a single service-owned transport boundary: a set of outbound publishing targets and a set of
/// inbound endpoint definitions. Outbound targets are executable routes selected by application publishing code;
/// inbound endpoints are framework-owned handler definitions selected by the inbound runtime after transport
/// inspection. The topology therefore exposes symmetric listing and diagnostic lookup, but does not imply
/// symmetric dispatch behavior.
/// </summary>
public interface ITopology
{
    TopologyName TopologyName { get; }

    IReadOnlyCollection<OutboundTarget> OutboundTargets { get; }

    IReadOnlyCollection<InboundEndpoint> InboundEndpoints { get; }

    OutboundTarget GetRequiredTarget(Type messageType);

    OutboundTarget<T> GetRequiredTarget<T>();

    bool TryGetTarget(Type messageType, out OutboundTarget? target);

    OutboundTarget GetRequiredTarget(string name);

    OutboundTarget<T> GetRequiredTarget<T>(string name);

    bool TryGetTarget(string name, out OutboundTarget? target);

    InboundEndpoint GetRequiredEndpoint(string name);

    bool TryGetEndpoint(string name, out InboundEndpoint? endpoint);
}
