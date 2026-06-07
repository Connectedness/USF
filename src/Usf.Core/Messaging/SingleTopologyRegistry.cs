using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

/// <summary>
/// A registry that exposes exactly one topology under <see cref="TopologyName.Default" />. It is primarily useful
/// for direct construction in tests and benchmarks where building a full service provider would be overkill.
/// </summary>
public sealed class SingleTopologyRegistry : ITopologyRegistry
{
    private readonly ITopology _topology;

    public SingleTopologyRegistry(ITopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        Names = [TopologyName.Default];
    }

    public IReadOnlyCollection<TopologyName> Names { get; }

    public ITopology GetRequiredTopology(TopologyName name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"Topology '{name.Value}' is not registered. Registered topologies: {TopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out ITopology? topology)
    {
        if (name == TopologyName.Default)
        {
            topology = _topology;
            return true;
        }

        topology = default;
        return false;
    }
}
