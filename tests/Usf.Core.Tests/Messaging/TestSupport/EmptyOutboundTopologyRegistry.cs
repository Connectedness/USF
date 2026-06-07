using System;
using System.Collections.Generic;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class EmptyOutboundTopologyRegistry : ITopologyRegistry
{
    public IReadOnlyCollection<TopologyName> Names { get; } = [];

    public ITopology GetRequiredTopology(TopologyName name)
    {
        throw new InvalidOperationException(
            $"Topology '{name.Value}' is not registered. Registered topologies: {TopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out ITopology? topology)
    {
        topology = null;
        return false;
    }
}
