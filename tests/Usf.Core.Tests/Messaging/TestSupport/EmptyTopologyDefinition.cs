using System;
using System.Collections.Generic;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

/// <summary>
/// Builds an empty <see cref="TopologyDefinition" /> for tests that need a topology with no outbound targets and
/// no inbound endpoints. <see cref="TopologyDefinition" /> is a concrete immutable data structure, so tests
/// instantiate it directly instead of relying on a hand-written fake.
/// </summary>
public static class EmptyTopologyDefinition
{
    public static TopologyDefinition Create()
    {
        return new TopologyDefinition(
            TopologyName.Default,
            new Dictionary<Type, OutboundTarget>(),
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal),
            new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
        );
    }
}
