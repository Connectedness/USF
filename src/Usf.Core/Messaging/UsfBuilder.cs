using System;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public sealed class UsfBuilder
{
    public UsfBuilder(
        IServiceCollection services,
        MessageContractRegistryBuilder messageContracts,
        OutboundTopologyRegistrationCatalog outboundTopologies,
        InboundTopologyRegistrationCatalog inboundTopologies
    )
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
        MessageContracts = messageContracts ?? throw new ArgumentNullException(nameof(messageContracts));
        OutboundTopologies = outboundTopologies ?? throw new ArgumentNullException(nameof(outboundTopologies));
        InboundTopologies = inboundTopologies ?? throw new ArgumentNullException(nameof(inboundTopologies));
    }

    public IServiceCollection Services { get; }

    public MessageContractRegistryBuilder MessageContracts { get; }

    public OutboundTopologyRegistrationCatalog OutboundTopologies { get; }

    public InboundTopologyRegistrationCatalog InboundTopologies { get; }

    public UsfBuilder MapMessageContracts(Action<MessageContractRegistryBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        configure(MessageContracts);
        return this;
    }

    public UsfBuilder UseCloudEvents(Action<CloudEventsOptions> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        Services.Configure(configure);
        return this;
    }
}
