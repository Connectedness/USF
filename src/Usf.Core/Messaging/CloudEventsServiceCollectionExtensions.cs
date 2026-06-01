using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Usf.Core.Messaging.Serialization;

namespace Usf.Core.Messaging;

public static class CloudEventsServiceCollectionExtensions
{
    public static IServiceCollection AddCloudEvents(
        this IServiceCollection services,
        Action<CloudEventsOptions> configureOptions,
        Action<MessageContractRegistryBuilder> configureContracts
    )
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configureOptions is null)
        {
            throw new ArgumentNullException(nameof(configureOptions));
        }

        if (configureContracts is null)
        {
            throw new ArgumentNullException(nameof(configureContracts));
        }

        MessageContractRegistryBuilder registryBuilder = new ();
        configureContracts(registryBuilder);
        var registry = registryBuilder.Build();

        services.AddOptions<CloudEventsOptions>()
           .Configure(configureOptions)
           .Validate(
                static options => CloudEventsOptionsValidation.IsValidSource(options.Source),
                "CloudEventsOptions.Source must be a non-empty URI-reference. Configure CloudEventsOptions.Source or pass a per-call CloudEventMetadata.Source override."
            )
           .ValidateOnStart();
        services.TryAddSingleton(
            static serviceProvider => serviceProvider.GetRequiredService<IOptions<CloudEventsOptions>>().Value
        );
        services.TryAddSingleton(registry);
        services.TryAddSingleton<IPayloadCodec, Utf8JsonPayloadCodec>();
        services.TryAddSingleton<CloudEventMessageSerializer>();
        services.TryAddSingleton<IMessageSerializer>(
            static serviceProvider => serviceProvider.GetRequiredService<CloudEventMessageSerializer>()
        );

        return services;
    }
}
