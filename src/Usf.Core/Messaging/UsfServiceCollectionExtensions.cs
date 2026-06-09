using System;
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
            throw new ArgumentNullException(nameof(services));
        }

        var messageContracts = GetOrAddMessageContracts(services);
        var topologies = GetOrAddTopologies(services);

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
        services.TryAddSingleton<ITopologyRegistry, TopologyRegistry>();
        services.TryAddSingleton<CloudEventsInboundMessageInspector>();
        services.TryAddSingleton<FrameworkMessageAcknowledgementMiddleware>();
        services.TryAddSingleton<MessageDeserializationMiddleware>();
        services.TryAddSingleton<MessageHandlerInvoker>();
        services.TryAddSingleton<IMessagePublisher>(
            static serviceProvider => new MessagePublisher(
                serviceProvider.GetRequiredService<ITopologyRegistry>()
            )
        );
        services.TryAddSingleton<Topology>(
            static serviceProvider => serviceProvider
               .GetRequiredService<ITopologyRegistry>()
               .GetRequiredTopology(Topology.DefaultName)
        );
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TopologyProvisioningHostedService>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TopologyRuntimeHostedService>());

        return new UsfBuilder(services, messageContracts, topologies);
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

    private static TopologyRegistrationCatalog GetOrAddTopologies(IServiceCollection services)
    {
        var existing = services
           .Select(static descriptor => descriptor.ImplementationInstance)
           .OfType<TopologyRegistrationCatalog>()
           .FirstOrDefault();

        if (existing is not null)
        {
            return existing;
        }

        TopologyRegistrationCatalog catalog = new ();
        services.TryAddSingleton(catalog);
        return catalog;
    }
}
