using System;

namespace Usf.Core.Messaging;

public sealed class OutboundTopologyRegistry : TopologyRegistry<IOutboundTopology>, IOutboundTopologyRegistry
{
    public OutboundTopologyRegistry(
        IServiceProvider serviceProvider,
        OutboundTopologyRegistrationCatalog catalog
    ) : base(serviceProvider, catalog) { }

    protected override string Direction => "outbound";
}
