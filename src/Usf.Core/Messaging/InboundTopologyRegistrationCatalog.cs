namespace Usf.Core.Messaging;

public sealed class InboundTopologyRegistrationCatalog : TopologyRegistrationCatalog
{
    protected override string Direction => "inbound";
}
