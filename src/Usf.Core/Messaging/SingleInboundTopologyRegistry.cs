namespace Usf.Core.Messaging;

public sealed class SingleInboundTopologyRegistry : SingleTopologyRegistry<IInboundTopology>, IInboundTopologyRegistry
{
    public SingleInboundTopologyRegistry(IInboundTopology topology) : base(topology) { }

    protected override string Direction => "inbound";
}
