using System;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public sealed class UsfBuilder
{
    public UsfBuilder(
        IServiceCollection services,
        MessageContractRegistryBuilder messageContracts,
        TopologyRegistrationCatalog topologies
    )
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
        MessageContracts = messageContracts ?? throw new ArgumentNullException(nameof(messageContracts));
        Topologies = topologies ?? throw new ArgumentNullException(nameof(topologies));
    }

    public IServiceCollection Services { get; }

    public MessageContractRegistryBuilder MessageContracts { get; }

    public TopologyRegistrationCatalog Topologies { get; }

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
