using System;
using System.Collections.Generic;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class TestTopology : Topology
{
    public TestTopology(
        string name,
        IDictionary<Type, OutboundTarget>? targetsByMessageType = null,
        IDictionary<string, OutboundTarget>? targetsByName = null,
        IDictionary<string, InboundEndpoint>? endpointsByName = null
    ) : base(
        name,
        TopologyData.PrepareTopologyDataStructures(
            targetsByMessageType ?? new Dictionary<Type, OutboundTarget>(),
            targetsByName ?? new Dictionary<string, OutboundTarget>(StringComparer.Ordinal),
            endpointsByName ?? new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
        )
    ) { }
}
