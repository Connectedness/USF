using System;
using System.Collections.Generic;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class EmptyOutboundTopology : ITopology
{
    public TopologyName TopologyName => TopologyName.Default;

    public IReadOnlyCollection<OutboundTarget> OutboundTargets => [];

    public IReadOnlyCollection<InboundEndpoint> InboundEndpoints => [];

    public OutboundTarget GetRequiredTarget(Type messageType)
    {
        throw new OutboundTargetNotFoundException(messageType);
    }

    public OutboundTarget<T> GetRequiredTarget<T>()
    {
        throw new OutboundTargetNotFoundException(typeof(T));
    }

    public bool TryGetTarget(Type messageType, out OutboundTarget? target)
    {
        target = null;
        return false;
    }

    public OutboundTarget GetRequiredTarget(string name)
    {
        throw new OutboundTargetNotFoundException(name);
    }

    public OutboundTarget<T> GetRequiredTarget<T>(string name)
    {
        throw new OutboundTargetNotFoundException(name);
    }

    public bool TryGetTarget(string name, out OutboundTarget? target)
    {
        target = null;
        return false;
    }

    public InboundEndpoint GetRequiredEndpoint(string name)
    {
        throw new InboundEndpointNotFoundException(name);
    }

    public bool TryGetEndpoint(string name, out InboundEndpoint? endpoint)
    {
        endpoint = null;
        return false;
    }
}
