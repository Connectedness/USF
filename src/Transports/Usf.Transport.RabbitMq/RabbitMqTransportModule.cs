using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqTransportModule
{
    public static UsfBuilder AddRabbitMqInboundTopology(
        this UsfBuilder builder,
        Action<RabbitMqInboundTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqInboundTopology(TopologyName.Default, configure);
    }

    public static UsfBuilder AddRabbitMqInboundTopology(
        this UsfBuilder builder,
        TopologyName topologyName,
        Action<RabbitMqInboundTopologyBuilder> configure
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

        var topologyBuilder = new RabbitMqInboundTopologyBuilder();
        configure(topologyBuilder);
        var configuration = topologyBuilder.Build();
        var services = builder.Services;
        builder.InboundTopologies.Add(topologyName);

        services.AddKeyedSingleton<RabbitMqInboundTopology>(
            topologyName,
            (serviceProvider, _) =>
            {
                var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
                RabbitMqConnectionProvider connectionProvider = new (
                    cancellationToken => CreateInboundConnectionAsync(
                        configuration,
                        serviceProvider,
                        cancellationToken
                    ),
                    loggerFactory.CreateLogger<RabbitMqConnectionProvider>()
                );
                RabbitMqInboundTopologyCompiler compiler = new (
                    serviceProvider.GetRequiredService<IMessageContractRegistry>(),
                    loggerFactory,
                    serviceType => IsServiceRegistered(serviceProvider, serviceType)
                );
                return compiler.Compile(topologyName, configuration, connectionProvider);
            }
        );
        services.AddKeyedSingleton<IInboundTopology>(
            topologyName,
            (serviceProvider, _) => serviceProvider
               .GetRequiredKeyedService<RabbitMqInboundTopology>(topologyName)
               .InboundTopology
        );
        services.AddKeyedSingleton<InboundTopology>(
            topologyName,
            (serviceProvider, _) => serviceProvider
               .GetRequiredKeyedService<RabbitMqInboundTopology>(topologyName)
               .InboundTopology
        );
        if (topologyName == TopologyName.Default)
        {
            services.TryAddSingleton<RabbitMqInboundTopology>(
                serviceProvider => serviceProvider.GetRequiredKeyedService<RabbitMqInboundTopology>(topologyName)
            );
        }

        services.AddSingleton<ITopologyProvisioner>(
            serviceProvider => new RabbitMqInboundTopologyProvisioner(
                serviceProvider.GetRequiredKeyedService<RabbitMqInboundTopology>(topologyName)
            )
        );
        services.AddSingleton<IHostedService>(
            serviceProvider => new RabbitMqInboundConsumerHostedService(
                serviceProvider.GetRequiredKeyedService<RabbitMqInboundTopology>(topologyName),
                serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                serviceProvider.GetService<ILogger<RabbitMqInboundConsumerHostedService>>()
            )
        );

        return builder;
    }

    public static UsfBuilder AddRabbitMqOutboundTopology(
        this UsfBuilder builder,
        Action<RabbitMqOutboundTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqOutboundTopology(TopologyName.Default, configure);
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
        services.AddSingleton<ITopologyProvisioner>(
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

    private static Task<IConnection> CreateInboundConnectionAsync(
        RabbitMqInboundTopologyConfiguration configuration,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken
    )
    {
        var connectionFactory = configuration.CreateConnectionFactory?.Invoke(serviceProvider) ??
                                throw new InboundTopologyValidationException(
                                    ["A RabbitMQ connection factory must be configured."]
                                );

        if (!connectionFactory.AutomaticRecoveryEnabled)
        {
            throw new InboundTopologyValidationException(
                [
                    "RabbitMQ automatic connection recovery must be enabled for inbound topologies. Configure ConnectionFactory.AutomaticRecoveryEnabled to true."
                ]
            );
        }

        if (!connectionFactory.TopologyRecoveryEnabled)
        {
            throw new InboundTopologyValidationException(
                [
                    "RabbitMQ topology recovery must be enabled for inbound topologies so RabbitMQ.Client can recover consumer subscriptions. Configure ConnectionFactory.TopologyRecoveryEnabled to true."
                ]
            );
        }

        return connectionFactory.CreateConnectionAsync(cancellationToken);
    }

    private static bool IsServiceRegistered(IServiceProvider serviceProvider, Type serviceType)
    {
        var serviceProviderIsService = serviceProvider.GetService<IServiceProviderIsService>();

        if (serviceProviderIsService is not null)
        {
            return serviceProviderIsService.IsService(serviceType);
        }

        return serviceProvider.GetService(serviceType) is not null;
    }
}
