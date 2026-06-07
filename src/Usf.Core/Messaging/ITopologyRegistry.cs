using System.Collections.Generic;

namespace Usf.Core.Messaging;

/// <summary>
/// Resolves registered topology instances by name. Both publishing topologies and consuming-only topologies are
/// reachable here for observability, tests, validation, and management APIs; the active behavior of consuming
/// topologies is started through <see cref="ITopologyRuntime" /> services rather than normal publish call sites.
/// </summary>
public interface ITopologyRegistry
{
    IReadOnlyCollection<TopologyName> Names { get; }

    ITopology GetRequiredTopology(TopologyName name);

    bool TryGetTopology(TopologyName name, out ITopology? topology);
}
