namespace Usf.Core.Messaging;

public sealed class SingleOutboundTopologyRegistry : SingleTopologyRegistry<IOutboundTopology>,
                                                     IOutboundTopologyRegistry
{
    public SingleOutboundTopologyRegistry(IOutboundTopology topology) : base(topology) { }

    protected override string Direction => "outbound";
}
