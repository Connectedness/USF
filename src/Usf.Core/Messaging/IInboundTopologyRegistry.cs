using System.Collections.Generic;

namespace Usf.Core.Messaging;

public interface IInboundTopologyRegistry
{
    IReadOnlyCollection<TopologyName> Names { get; }

    IInboundTopology GetRequiredTopology(TopologyName name);

    bool TryGetTopology(TopologyName name, out IInboundTopology? topology);
}
