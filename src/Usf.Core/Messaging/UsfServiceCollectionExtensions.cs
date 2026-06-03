using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Usf.Core.Messaging.Serialization;

namespace Usf.Core.Messaging;

public static class UsfServiceCollectionExtensions
{
    public static UsfBuilder AddUsf(this IServiceCollection services)
    {
        if (services is null)
        {
            throw new System.ArgumentNullException(nameof(services));
        }

        var messageContracts = GetOrAddMessageContracts(services);
        var outboundTopologies = GetOrAddOutboundTopologies(services);

        services.AddOptions<CloudEventsOptions>()
           .Validate(
                static options => CloudEventsOptionsValidation.IsValidSource(options.Source),
                "CloudEventsOptions.Source must be a non-empty URI-reference. Configure CloudEventsOptions.Source or pass a per-call CloudEventMetadata.Source override."
            )
           .ValidateOnStart();
        services.TryAddSingleton(
            static serviceProvider => serviceProvider.GetRequiredService<IOptions<CloudEventsOptions>>().Value
        );
        services.TryAddSingleton<IMessageContractRegistry>(
            static serviceProvider => serviceProvider.GetRequiredService<MessageContractRegistryBuilder>().Build()
        );
        services.TryAddSingleton<IPayloadCodec, Utf8JsonPayloadCodec>();
        services.TryAddSingleton<CloudEventMessageSerializer>();
        services.TryAddSingleton<IMessageSerializer>(
            static serviceProvider => serviceProvider.GetRequiredService<CloudEventMessageSerializer>()
        );
        services.TryAddSingleton<IOutboundTopologyRegistry, OutboundTopologyRegistry>();
        services.TryAddSingleton<IMessagePublisher>(
            static serviceProvider => new MessagePublisher(
                serviceProvider.GetRequiredService<IOutboundTopologyRegistry>()
            )
        );
        services.TryAddSingleton<IOutboundTopology>(
            static serviceProvider => serviceProvider
               .GetRequiredService<IOutboundTopologyRegistry>()
               .GetRequiredTopology(TopologyName.Default)
        );
        services.TryAddSingleton<OutboundTopology>(
            static serviceProvider => (OutboundTopology) serviceProvider
               .GetRequiredService<IOutboundTopologyRegistry>()
               .GetRequiredTopology(TopologyName.Default)
        );
        services.TryAddSingleton<IOutboundTargetRegistry>(
            static serviceProvider => serviceProvider
               .GetRequiredService<IOutboundTopologyRegistry>()
               .GetRequiredTopology(TopologyName.Default)
        );
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboundTopologyHostedService>());

        return new UsfBuilder(services, messageContracts, outboundTopologies);
    }

    private static MessageContractRegistryBuilder GetOrAddMessageContracts(IServiceCollection services)
    {
        var existing = services
           .Select(static descriptor => descriptor.ImplementationInstance)
           .OfType<MessageContractRegistryBuilder>()
           .FirstOrDefault();

        if (existing is not null)
        {
            return existing;
        }

        MessageContractRegistryBuilder builder = new ();
        services.TryAddSingleton(builder);
        return builder;
    }

    private static OutboundTopologyRegistrationCatalog GetOrAddOutboundTopologies(IServiceCollection services)
    {
        var existing = services
           .Select(static descriptor => descriptor.ImplementationInstance)
           .OfType<OutboundTopologyRegistrationCatalog>()
           .FirstOrDefault();

        if (existing is not null)
        {
            return existing;
        }

        OutboundTopologyRegistrationCatalog catalog = new ();
        services.TryAddSingleton(catalog);
        return catalog;
    }
}
