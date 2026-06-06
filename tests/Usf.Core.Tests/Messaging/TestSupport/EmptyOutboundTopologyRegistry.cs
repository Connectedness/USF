using System;
using System.Collections.Generic;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class EmptyOutboundTopologyRegistry : IOutboundTopologyRegistry
{
    public IReadOnlyCollection<TopologyName> Names { get; } = [];

    public IOutboundTopology GetRequiredTopology(TopologyName name)
    {
        throw new InvalidOperationException(
            $"Outbound topology '{name.Value}' is not registered. Registered outbound topologies: {OutboundTopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out IOutboundTopology? topology)
    {
        topology = null;
        return false;
    }
}
