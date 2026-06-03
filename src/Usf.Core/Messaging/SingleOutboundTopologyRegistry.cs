using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public sealed class SingleOutboundTopologyRegistry : IOutboundTopologyRegistry
{
    private readonly IOutboundTopology _topology;

    public SingleOutboundTopologyRegistry(IOutboundTopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        Names = [TopologyName.Default];
    }

    public IReadOnlyCollection<TopologyName> Names { get; }

    public IOutboundTopology GetRequiredTopology(TopologyName name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"Outbound topology '{name.Value}' is not registered. Registered outbound topologies: {OutboundTopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out IOutboundTopology? topology)
    {
        if (name == TopologyName.Default)
        {
            topology = _topology;
            return true;
        }

        topology = null;
        return false;
    }
}
