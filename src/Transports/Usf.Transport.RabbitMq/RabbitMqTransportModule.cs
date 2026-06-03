using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqTransportModule
{
    public static UsfBuilder AddRabbitMqOutboundTopology(
        this UsfBuilder builder,
        Action<RabbitMqOutboundTopologyBuilder> configure
    )
    {
        return AddRabbitMqOutboundTopology(builder, TopologyName.Default, configure);
    }

    public static UsfBuilder AddRabbitMqOutboundTopology(
        this UsfBuilder builder,
        TopologyName topologyName,
        Action<RabbitMqOutboundTopologyBuilder> configure
    )
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        var topologyBuilder = new RabbitMqOutboundTopologyBuilder();
        configure(topologyBuilder);
        var configuration = topologyBuilder.Build();
        var services = builder.Services;
        builder.OutboundTopologies.Add(topologyName);

        services.AddKeyedSingleton<RabbitMqOutboundTopology>(
            topologyName,
            (serviceProvider, _) =>
            {
                var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
                RabbitMqConnectionProvider connectionProvider = new (
                    cancellationToken => CreateConnectionAsync(configuration, serviceProvider, cancellationToken),
                    loggerFactory.CreateLogger<RabbitMqConnectionProvider>()
                );
                RabbitMqOutboundTopologyCompiler compiler = new (
                    serviceProvider.GetRequiredService<IMessageContractRegistry>(),
                    loggerFactory,
                    serializerType => (IMessageSerializer?) serviceProvider.GetService(serializerType)
                );
                return compiler.Compile(topologyName, configuration, connectionProvider);
            }
        );
        services.AddKeyedSingleton<IOutboundTopology>(
            topologyName,
            (serviceProvider, _) => serviceProvider
               .GetRequiredKeyedService<RabbitMqOutboundTopology>(topologyName)
               .OutboundTopology
        );
        services.AddKeyedSingleton<OutboundTopology>(
            topologyName,
            (serviceProvider, _) => serviceProvider
               .GetRequiredKeyedService<RabbitMqOutboundTopology>(topologyName)
               .OutboundTopology
        );
        services.AddKeyedSingleton<IOutboundTargetRegistry>(
            topologyName,
            (serviceProvider, _) => serviceProvider
               .GetRequiredKeyedService<RabbitMqOutboundTopology>(topologyName)
               .OutboundTopology
        );
        if (topologyName == TopologyName.Default)
        {
            services.TryAddSingleton<RabbitMqOutboundTopology>(
                serviceProvider => serviceProvider.GetRequiredKeyedService<RabbitMqOutboundTopology>(topologyName)
            );
        }

        services.AddSingleton<IOutboundTopologyProvisioner>(
            serviceProvider => new RabbitMqOutboundTopologyProvisioner(
                serviceProvider.GetRequiredKeyedService<RabbitMqOutboundTopology>(topologyName)
            )
        );

        return builder;
    }

    private static Task<IConnection> CreateConnectionAsync(
        RabbitMqOutboundTopologyConfiguration configuration,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken
    )
    {
        var connectionFactory = configuration.CreateConnectionFactory?.Invoke(serviceProvider) ??
                                throw new OutboundTopologyValidationException(
                                    ["A RabbitMQ connection factory must be configured."]
                                );

        if (!connectionFactory.AutomaticRecoveryEnabled)
        {
            throw new OutboundTopologyValidationException(
                [
                    "RabbitMQ automatic connection recovery must be enabled. Configure ConnectionFactory.AutomaticRecoveryEnabled to true."
                ]
            );
        }

        return connectionFactory.CreateConnectionAsync(cancellationToken);
    }
}
