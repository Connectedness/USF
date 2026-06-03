using System.Collections.Generic;

namespace Usf.Core.Messaging;

public interface IOutboundTopologyRegistry
{
    IReadOnlyCollection<TopologyName> Names { get; }

    IOutboundTopology GetRequiredTopology(TopologyName name);

    bool TryGetTopology(TopologyName name, out IOutboundTopology? topology);
}
