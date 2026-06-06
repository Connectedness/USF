namespace Usf.Core.Messaging;

public sealed class OutboundTopologyRegistrationCatalog : TopologyRegistrationCatalog
{
    protected override string Direction => "outbound";
}
