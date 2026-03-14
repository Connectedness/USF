using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqTransportModule
{
    public static IServiceCollection AddRabbitMqMessagePublishing(
        this IServiceCollection services,
        Action<RabbitMqMessagePublishingBuilder> configure
    )
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        var builder = new RabbitMqMessagePublishingBuilder();
        configure(builder);
        var configuration = builder.Build();

        services.AddSingleton(configuration);
        services.AddSingleton<RabbitMqConnectionManager>();
        services.AddSingleton<RabbitMqCompiledTopology>(
            static serviceProvider => RabbitMqMessageTopologyCompiler.Compile(serviceProvider)
        );
        services.AddSingleton<IMessageTopology>(
            static serviceProvider => serviceProvider.GetRequiredService<RabbitMqCompiledTopology>().MessageTopology
        );
        services.AddSingleton<MessageTopology>(
            static serviceProvider => serviceProvider.GetRequiredService<RabbitMqCompiledTopology>().MessageTopology
        );
        services.AddSingleton<ITargetRegistry>(
            static serviceProvider => serviceProvider.GetRequiredService<RabbitMqCompiledTopology>().MessageTopology
        );
        services.AddSingleton<IMessagePublisher, MessagePublisher>();
        services.AddSingleton<ITopologyProvisioner, RabbitMqTopologyProvisioner>();
        services.AddSingleton<IHostedService, MessagePublishingHostedService>();

        return services;
    }
}
