using System;

namespace Usf.Core.Messaging;

public sealed class InboundTopologyRegistry : TopologyRegistry<IInboundTopology>, IInboundTopologyRegistry
{
    public InboundTopologyRegistry(
        IServiceProvider serviceProvider,
        InboundTopologyRegistrationCatalog catalog
    ) : base(serviceProvider, catalog) { }

    protected override string Direction => "inbound";
}
