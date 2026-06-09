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
    public static UsfBuilder AddRabbitMqTopology(
        this UsfBuilder builder,
        Action<RabbitMqTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqTopology(Topology.DefaultName, configure);
    }

    public static UsfBuilder AddRabbitMqTopology(
        this UsfBuilder builder,
        string topologyName,
        Action<RabbitMqTopologyBuilder> configure
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

        var topologyBuilder = new RabbitMqTopologyBuilder();
        configure(topologyBuilder);
        var configuration = topologyBuilder.Build();
        var services = builder.Services;
        builder.Topologies.Add(topologyName);

        services.AddKeyedSingleton<RabbitMqTopology>(
            topologyName,
            (serviceProvider, _) =>
            {
                var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
                RabbitMqConnectionProvider connectionProvider = new (
                    cancellationToken => CreateConnectionAsync(
                        configuration,
                        serviceProvider,
                        cancellationToken
                    ),
                    loggerFactory.CreateLogger<RabbitMqConnectionProvider>()
                );
                RabbitMqTopologyCompiler compiler = new (
                    serviceProvider.GetRequiredService<IMessageContractRegistry>(),
                    loggerFactory,
                    serializerType => (IMessageSerializer?) serviceProvider.GetService(serializerType),
                    serviceType => IsServiceRegistered(serviceProvider, serviceType)
                );
                return compiler.Compile(topologyName, configuration, connectionProvider);
            }
        );
        services.AddKeyedSingleton<Topology>(
            topologyName,
            (serviceProvider, key) => serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(key)
        );
        if (string.Equals(topologyName, Topology.DefaultName, StringComparison.Ordinal))
        {
            services.TryAddSingleton<RabbitMqTopology>(
                serviceProvider => serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(topologyName)
            );
        }

        services.AddSingleton<ITopologyProvisioner>(
            serviceProvider => new RabbitMqTopologyProvisioner(
                serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(topologyName)
            )
        );

        if (configuration.HasInboundEndpoints)
        {
            services.AddSingleton<ITopologyRuntime>(
                serviceProvider => new RabbitMqTopologyRuntime(
                    serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(topologyName),
                    serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                    serviceProvider.GetService<ILogger<RabbitMqTopologyRuntime>>()
                )
            );
        }

        return builder;
    }

    /// <summary>
    /// Compatibility wrapper that registers a publish-oriented RabbitMQ topology. It compiles to the same unified
    /// <see cref="RabbitMqTopology" /> as <see cref="AddRabbitMqTopology(UsfBuilder, Action{RabbitMqTopologyBuilder})" />.
    /// </summary>
    public static UsfBuilder AddRabbitMqOutboundTopology(
        this UsfBuilder builder,
        Action<RabbitMqTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqTopology(Topology.DefaultName, configure);
    }

    /// <inheritdoc cref="AddRabbitMqOutboundTopology(UsfBuilder, Action{RabbitMqTopologyBuilder})" />
    public static UsfBuilder AddRabbitMqOutboundTopology(
        this UsfBuilder builder,
        string topologyName,
        Action<RabbitMqTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqTopology(topologyName, configure);
    }

    /// <summary>
    /// Compatibility wrapper that registers a consume-oriented RabbitMQ topology. It compiles to the same unified
    /// <see cref="RabbitMqTopology" /> as <see cref="AddRabbitMqTopology(UsfBuilder, Action{RabbitMqTopologyBuilder})" />.
    /// </summary>
    public static UsfBuilder AddRabbitMqInboundTopology(
        this UsfBuilder builder,
        Action<RabbitMqTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqTopology(Topology.DefaultName, configure);
    }

    /// <inheritdoc cref="AddRabbitMqInboundTopology(UsfBuilder, Action{RabbitMqTopologyBuilder})" />
    public static UsfBuilder AddRabbitMqInboundTopology(
        this UsfBuilder builder,
        string topologyName,
        Action<RabbitMqTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqTopology(topologyName, configure);
    }

    private static Task<IConnection> CreateConnectionAsync(
        RabbitMqTopologyConfiguration configuration,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken
    )
    {
        var connectionFactory = configuration.CreateConnectionFactory?.Invoke(serviceProvider) ??
                                throw new TopologyValidationException(
                                    ["A RabbitMQ connection factory must be configured."]
                                );

        if (!connectionFactory.AutomaticRecoveryEnabled)
        {
            throw new TopologyValidationException(
                [
                    "RabbitMQ automatic connection recovery must be enabled. Configure ConnectionFactory.AutomaticRecoveryEnabled to true."
                ]
            );
        }

        if (configuration.HasInboundEndpoints && !connectionFactory.TopologyRecoveryEnabled)
        {
            throw new TopologyValidationException(
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
