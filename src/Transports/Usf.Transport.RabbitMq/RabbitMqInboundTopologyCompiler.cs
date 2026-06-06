using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundTopologyCompiler
{
    private static readonly MethodInfo CreateEndpointMethod = typeof(RabbitMqInboundTopologyCompiler)
       .GetMethod(nameof(CreateEndpointCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private readonly IMessageContractRegistry _canonicalMessageContracts;
    private readonly Func<Type, bool> _isServiceRegistered;
    private readonly ILoggerFactory _loggerFactory;

    public RabbitMqInboundTopologyCompiler(
        IMessageContractRegistry canonicalMessageContracts,
        ILoggerFactory loggerFactory,
        Func<Type, bool> isServiceRegistered
    )
    {
        _canonicalMessageContracts = canonicalMessageContracts ??
                                     throw new ArgumentNullException(nameof(canonicalMessageContracts));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _isServiceRegistered = isServiceRegistered ?? throw new ArgumentNullException(nameof(isServiceRegistered));
    }

    public RabbitMqInboundTopology Compile(
        TopologyName topologyName,
        RabbitMqInboundTopologyConfiguration configuration,
        RabbitMqConnectionProvider connectionProvider
    )
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (connectionProvider is null)
        {
            throw new ArgumentNullException(nameof(connectionProvider));
        }

        var effectiveMessageContracts = CreateEffectiveMessageContracts(configuration);
        var validationErrors = Validate(configuration, effectiveMessageContracts);

        if (validationErrors.Count > 0)
        {
            throw new InboundTopologyValidationException(validationErrors);
        }

        RabbitMqChannelSource channelSource = new (
            connectionProvider,
            "inbound",
            static validationErrors => new InboundTopologyValidationException(validationErrors)
        );

        Dictionary<string, RabbitMqInboundChannelGroup> explicitChannelGroupsByName =
            new (StringComparer.Ordinal);
        List<RabbitMqInboundChannelGroup> channelGroups = [];
        Dictionary<string, InboundEndpoint> endpointsByName = new (StringComparer.Ordinal);
        Dictionary<InboundEndpointSelectionKey, InboundEndpoint> neutralDispatchIndex =
            new (InboundEndpointSelectionKeyComparer.Instance);
        Dictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> dispatchIndex =
            new (InboundEndpointSelectionKeyComparer.Instance);
        List<RabbitMqInboundEndpoint> endpoints = [];

        foreach (var channelGroupDefinition in OrderChannelGroups(configuration.ChannelGroups))
        {
            var channelGroup = CreateChannelGroup(channelGroupDefinition);
            explicitChannelGroupsByName.Add(channelGroup.Name, channelGroup);
            channelGroups.Add(channelGroup);
        }

        foreach (var handlerDefinition in OrderHandlers(configuration.Handlers))
        {
            var canonicalDiscriminator = effectiveMessageContracts.GetDiscriminator(handlerDefinition.MessageType);
            var inboundDiscriminators = effectiveMessageContracts.GetInboundDiscriminators(
                handlerDefinition.MessageType
            );
            var endpointName = handlerDefinition.EndpointName ??
                               $"{handlerDefinition.QueueName}:{canonicalDiscriminator}";
            var channelGroup = ResolveChannelGroup(
                handlerDefinition,
                endpointName,
                explicitChannelGroupsByName,
                channelGroups
            );
            var endpoint = CreateEndpoint(
                handlerDefinition,
                topologyName,
                endpointName,
                canonicalDiscriminator,
                channelGroup
            );

            endpoints.Add(endpoint);
            endpointsByName.Add(endpoint.Name, endpoint);

            foreach (var discriminator in inboundDiscriminators)
            {
                var dispatchKey = new InboundEndpointSelectionKey(handlerDefinition.QueueName, discriminator);
                dispatchIndex.Add(dispatchKey, endpoint);
                neutralDispatchIndex.Add(dispatchKey, endpoint);
            }
        }

        var (worstCaseChannelCount, description) = RabbitMqInboundChannelBudget.Calculate(channelGroups);
        channelSource.SetChannelBudget(worstCaseChannelCount, description);
        LogWorstCaseChannelCount(worstCaseChannelCount, description);

        return new RabbitMqInboundTopology(
            new InboundTopology(endpointsByName, neutralDispatchIndex),
            effectiveMessageContracts,
            configuration.Exchanges,
            configuration.Queues,
            configuration.Bindings,
            channelGroups.AsReadOnly(),
            endpoints.AsReadOnly(),
            new ReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint>(dispatchIndex),
            BuildPipeline(configuration),
            configuration.ShutdownTimeout,
            connectionProvider,
            channelSource
        );
    }

    private IMessageContractRegistry CreateEffectiveMessageContracts(
        RabbitMqInboundTopologyConfiguration configuration
    )
    {
        return configuration.MessageContractDialect is null ?
            _canonicalMessageContracts :
            new EffectiveMessageContractRegistry(
                _canonicalMessageContracts,
                configuration.MessageContractDialect
            );
    }

    private static MessageDelegate BuildPipeline(RabbitMqInboundTopologyConfiguration configuration)
    {
        MessagePipelineBuilder pipeline = new ();
        pipeline.UseMiddleware<FrameworkMessageAcknowledgementMiddleware>();
        pipeline.Use(
            next => async context =>
            {
                var middleware = (IMessageMiddleware) context.Services.GetRequiredService(
                    configuration.DeserializationMiddlewareType
                );
                await middleware.InvokeAsync(context, next).ConfigureAwait(false);
            }
        );
        configuration.ConfigurePipeline?.Invoke(pipeline);
        return pipeline.Build(
            static context => context.Services.GetRequiredService<MessageHandlerInvoker>().InvokeAsync(context)
        );
    }

    private static IEnumerable<RabbitMqInboundChannelGroupDefinition> OrderChannelGroups(
        IReadOnlyList<RabbitMqInboundChannelGroupDefinition> channelGroups
    )
    {
        return channelGroups.OrderBy(static channelGroup => channelGroup.Name, StringComparer.Ordinal);
    }

    private static IEnumerable<RabbitMqInboundHandlerDefinition> OrderHandlers(
        IReadOnlyList<RabbitMqInboundHandlerDefinition> handlers
    )
    {
        return handlers
           .OrderBy(static handler => handler.QueueName, StringComparer.Ordinal)
           .ThenBy(static handler => handler.MessageType.AssemblyQualifiedName, StringComparer.Ordinal)
           .ThenBy(static handler => handler.EndpointName ?? string.Empty, StringComparer.Ordinal);
    }

    private static RabbitMqInboundChannelGroup CreateChannelGroup(
        RabbitMqInboundChannelGroupDefinition definition
    )
    {
        return new RabbitMqInboundChannelGroup(
            definition.Name,
            definition.MaximumChannelCount,
            definition.PrefetchCount,
            definition.ConsumerDispatchConcurrency
        );
    }

    private static RabbitMqInboundChannelGroup ResolveChannelGroup(
        RabbitMqInboundHandlerDefinition handlerDefinition,
        string endpointName,
        IReadOnlyDictionary<string, RabbitMqInboundChannelGroup> explicitChannelGroupsByName,
        ICollection<RabbitMqInboundChannelGroup> channelGroups
    )
    {
        if (!string.IsNullOrWhiteSpace(handlerDefinition.ChannelGroupName))
        {
            return explicitChannelGroupsByName[handlerDefinition.ChannelGroupName!];
        }

        var implicitChannelGroup = new RabbitMqInboundChannelGroup(
            $"{RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix}{channelGroups.Count}:{endpointName}",
            handlerDefinition.ChannelCount,
            handlerDefinition.PrefetchCount,
            handlerDefinition.ConsumerDispatchConcurrency
        );
        channelGroups.Add(implicitChannelGroup);
        return implicitChannelGroup;
    }

    private static RabbitMqInboundEndpoint CreateEndpoint(
        RabbitMqInboundHandlerDefinition handlerDefinition,
        TopologyName topologyName,
        string endpointName,
        string discriminator,
        RabbitMqInboundChannelGroup channelGroup
    )
    {
        var closedMethod = CreateEndpointMethod.MakeGenericMethod(handlerDefinition.MessageType);
        return (RabbitMqInboundEndpoint) closedMethod.Invoke(
            null,
            [handlerDefinition, topologyName, endpointName, discriminator, channelGroup]
        )!;
    }

    private static RabbitMqInboundEndpoint CreateEndpointCore<TMessage>(
        RabbitMqInboundHandlerDefinition handlerDefinition,
        TopologyName topologyName,
        string endpointName,
        string discriminator,
        RabbitMqInboundChannelGroup channelGroup
    )
    {
        return new RabbitMqInboundEndpoint<TMessage>(
            endpointName,
            topologyName,
            handlerDefinition.HandlerType,
            handlerDefinition.SerializerType,
            discriminator,
            handlerDefinition.AckMode,
            handlerDefinition.QueueName,
            handlerDefinition.InspectorType,
            channelGroup
        );
    }

    private List<string> Validate(
        RabbitMqInboundTopologyConfiguration configuration,
        IMessageContractRegistry effectiveMessageContracts
    )
    {
        List<string> validationErrors = [];

        if (configuration.CreateConnectionFactory is null)
        {
            validationErrors.Add("A RabbitMQ connection factory must be configured.");
        }

        if (configuration.Handlers.Count == 0)
        {
            validationErrors.Add("At least one RabbitMQ inbound handler must be configured.");
        }

        validationErrors.AddRange(
            FindDuplicateNames(configuration.Exchanges.Select(static exchange => exchange.Name), "exchange")
        );
        validationErrors.AddRange(FindDuplicateNames(configuration.Queues.Select(static queue => queue.Name), "queue"));
        validationErrors.AddRange(
            FindDuplicateNames(configuration.ChannelGroups.Select(static group => group.Name), "channel group")
        );

        var exchangesByName = ToDictionary(configuration.Exchanges, static exchange => exchange.Name);
        var queuesByName = ToDictionary(configuration.Queues, static queue => queue.Name);
        var channelGroupsByName = ToDictionary(configuration.ChannelGroups, static group => group.Name);

        ValidateExchangeDefinitions(configuration.Exchanges, validationErrors);
        ValidateQueueDefinitions(configuration.Queues, validationErrors);
        ValidateChannelGroupDefinitions(configuration.ChannelGroups, validationErrors);
        ValidateChannelGroupUsage(configuration.ChannelGroups, configuration.Handlers, validationErrors);
        ValidatePipeline(configuration, validationErrors);
        ValidateBindings(configuration.Bindings, exchangesByName, queuesByName, validationErrors);
        ValidateHandlers(
            configuration.Handlers,
            queuesByName,
            channelGroupsByName,
            effectiveMessageContracts,
            validationErrors
        );

        return validationErrors;
    }

    private static void ValidateExchangeDefinitions(
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        ICollection<string> validationErrors
    )
    {
        foreach (var exchange in exchanges.OrderBy(static exchange => exchange.Name, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqDeclareMode), exchange.DeclareMode))
            {
                validationErrors.Add(
                    $"Exchange '{exchange.Name}' uses unsupported declare mode '{exchange.DeclareMode}'."
                );
            }

            if (string.Equals(exchange.Type, "internal", StringComparison.OrdinalIgnoreCase))
            {
                validationErrors.Add(
                    $"Exchange '{exchange.Name}' uses unsupported exchange type 'internal'."
                );
            }
        }
    }

    private static void ValidateQueueDefinitions(
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        ICollection<string> validationErrors
    )
    {
        foreach (var queue in queues.OrderBy(static queue => queue.Name, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqDeclareMode), queue.DeclareMode))
            {
                validationErrors.Add(
                    $"Queue '{queue.Name}' uses unsupported declare mode '{queue.DeclareMode}'."
                );
            }
        }
    }

    private static void ValidateChannelGroupDefinitions(
        IReadOnlyList<RabbitMqInboundChannelGroupDefinition> channelGroups,
        ICollection<string> validationErrors
    )
    {
        foreach (var channelGroup in channelGroups.OrderBy(static group => group.Name, StringComparer.Ordinal))
        {
            if (channelGroup.MaximumChannelCount < 1)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' maximum channel count must be greater than zero."
                );
            }

            if (channelGroup.PrefetchCount == 0)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' prefetch count must be greater than zero."
                );
            }

            if (channelGroup.ConsumerDispatchConcurrency == 0)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' consumer dispatch concurrency must be greater than zero."
                );
            }

            if (channelGroup.Name.StartsWith(
                    RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix,
                    StringComparison.Ordinal
                ))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' uses reserved name prefix '{RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix}'."
                );
            }
        }
    }

    private static void ValidateChannelGroupUsage(
        IReadOnlyList<RabbitMqInboundChannelGroupDefinition> channelGroups,
        IReadOnlyList<RabbitMqInboundHandlerDefinition> handlers,
        ICollection<string> validationErrors
    )
    {
        var referencedChannelGroups = new HashSet<string>(
            handlers
               .Where(static handler => !string.IsNullOrWhiteSpace(handler.ChannelGroupName))
               .Select(static handler => handler.ChannelGroupName!),
            StringComparer.Ordinal
        );

        foreach (var channelGroupName in channelGroups
                    .Select(static group => group.Name)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(static name => name, StringComparer.Ordinal))
        {
            if (!referencedChannelGroups.Contains(channelGroupName))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroupName}' is configured but no inbound endpoint references it."
                );
            }
        }
    }

    private void ValidateHandlers(
        IReadOnlyList<RabbitMqInboundHandlerDefinition> handlers,
        IReadOnlyDictionary<string, RabbitMqQueueDefinition> queuesByName,
        IReadOnlyDictionary<string, RabbitMqInboundChannelGroupDefinition> channelGroupsByName,
        IMessageContractRegistry effectiveMessageContracts,
        ICollection<string> validationErrors
    )
    {
        Dictionary<string, RabbitMqInboundHandlerDefinition> endpointNames = new (StringComparer.Ordinal);
        HashSet<InboundEndpointSelectionKey> dispatchKeys = new (InboundEndpointSelectionKeyComparer.Instance);

        foreach (var handler in OrderHandlers(handlers))
        {
            if (!queuesByName.ContainsKey(handler.QueueName))
            {
                validationErrors.Add(
                    $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' references unknown queue '{handler.QueueName}'."
                );
            }

            if (!string.IsNullOrWhiteSpace(handler.ChannelGroupName) &&
                !channelGroupsByName.ContainsKey(handler.ChannelGroupName!))
            {
                validationErrors.Add(
                    $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' references unknown channel group '{handler.ChannelGroupName}'."
                );
            }

            ValidateServiceRegistrations(handler, validationErrors);
            ValidateAckMode(handler, validationErrors);

            IReadOnlyCollection<string> inboundDiscriminators;
            string canonicalDiscriminator;

            try
            {
                canonicalDiscriminator = effectiveMessageContracts.GetDiscriminator(handler.MessageType);
                inboundDiscriminators = effectiveMessageContracts.GetInboundDiscriminators(handler.MessageType);
            }
            catch (MessageContractNotRegisteredException)
            {
                validationErrors.Add(
                    $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' consumes unregistered CloudEvents message type. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...)."
                );
                continue;
            }

            if (inboundDiscriminators.Count == 0)
            {
                validationErrors.Add(
                    $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' has no inbound CloudEvents discriminators. Use MessageContractRegistryBuilder.Map<T>(...) instead of MapOutbound<T>(...)."
                );
                continue;
            }

            var endpointName = handler.EndpointName ?? $"{handler.QueueName}:{canonicalDiscriminator}";

            if (!endpointNames.TryAdd(endpointName, handler))
            {
                validationErrors.Add($"Inbound endpoint name '{endpointName}' is configured multiple times.");
            }

            foreach (var discriminator in inboundDiscriminators)
            {
                var dispatchKey = new InboundEndpointSelectionKey(handler.QueueName, discriminator);

                if (!dispatchKeys.Add(dispatchKey))
                {
                    validationErrors.Add(
                        $"Inbound endpoint discriminator '{discriminator}' is configured multiple times for queue '{handler.QueueName}'."
                    );
                }
            }
        }
    }

    private void ValidateServiceRegistrations(
        RabbitMqInboundHandlerDefinition handler,
        ICollection<string> validationErrors
    )
    {
        var handlerServiceType = typeof(IMessageHandler<>).MakeGenericType(handler.MessageType);

        if (!_isServiceRegistered(handlerServiceType))
        {
            validationErrors.Add(
                $"Inbound handler service '{handlerServiceType}' for message '{GetTypeName(handler.MessageType)}' is not registered."
            );
        }

        if (!_isServiceRegistered(handler.SerializerType))
        {
            validationErrors.Add(
                $"Inbound serializer '{handler.SerializerType}' for message '{GetTypeName(handler.MessageType)}' is not registered."
            );
        }

        if (!_isServiceRegistered(handler.InspectorType))
        {
            validationErrors.Add(
                $"Inbound inspector '{handler.InspectorType}' for queue '{handler.QueueName}' is not registered."
            );
        }
    }

    private void ValidatePipeline(
        RabbitMqInboundTopologyConfiguration configuration,
        ICollection<string> validationErrors
    )
    {
        if (!typeof(IMessageMiddleware).IsAssignableFrom(configuration.DeserializationMiddlewareType))
        {
            validationErrors.Add(
                $"Inbound deserialization middleware '{configuration.DeserializationMiddlewareType}' must implement '{typeof(IMessageMiddleware)}'."
            );
            return;
        }

        if (!_isServiceRegistered(configuration.DeserializationMiddlewareType))
        {
            validationErrors.Add(
                $"Inbound deserialization middleware '{configuration.DeserializationMiddlewareType}' is not registered."
            );
        }
    }

    private static void ValidateAckMode(
        RabbitMqInboundHandlerDefinition handler,
        ICollection<string> validationErrors
    )
    {
        if (!Enum.IsDefined(typeof(MessageAckMode), handler.AckMode))
        {
            validationErrors.Add(
                $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' uses unsupported acknowledgement mode '{handler.AckMode}'."
            );
        }
    }

    private static void ValidateBindings(
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqQueueDefinition> queuesByName,
        ICollection<string> validationErrors
    )
    {
        foreach (var binding in bindings.OrderBy(static binding => binding.SourceExchangeName, StringComparer.Ordinal)
                    .ThenBy(static binding => binding.RoutingKey, StringComparer.Ordinal))
        {
            if (!exchangesByName.ContainsKey(binding.SourceExchangeName))
            {
                validationErrors.Add(
                    $"Binding references unknown source exchange '{binding.SourceExchangeName}'."
                );
            }

            switch (binding)
            {
                case RabbitMqQueueBindingDefinition queueBinding:
                    if (!queuesByName.ContainsKey(queueBinding.QueueName))
                    {
                        validationErrors.Add(
                            $"Binding from exchange '{queueBinding.SourceExchangeName}' references unknown queue '{queueBinding.QueueName}'."
                        );
                    }

                    break;
                case RabbitMqExchangeBindingDefinition exchangeBinding:
                    if (!exchangesByName.ContainsKey(exchangeBinding.DestinationExchangeName))
                    {
                        validationErrors.Add(
                            $"Binding from exchange '{exchangeBinding.SourceExchangeName}' references unknown destination exchange '{exchangeBinding.DestinationExchangeName}'."
                        );
                    }

                    break;
            }
        }
    }

    private static IReadOnlyDictionary<string, T> ToDictionary<T>(
        IEnumerable<T> values,
        Func<T, string> getName
    )
    {
        Dictionary<string, T> dictionary = new (StringComparer.Ordinal);

        foreach (var value in values)
        {
            var name = getName(value);

            if (!dictionary.ContainsKey(name))
            {
                dictionary.Add(name, value);
            }
        }

        return dictionary;
    }

    private static IEnumerable<string> FindDuplicateNames(IEnumerable<string> names, string resourceName)
    {
        return names
           .GroupBy(static name => name, StringComparer.Ordinal)
           .Where(static group => group.Count() > 1)
           .OrderBy(static group => group.Key, StringComparer.Ordinal)
           .Select(group => $"{ToSentenceCase(resourceName)} '{group.Key}' is configured multiple times.");
    }

    private static string GetTypeName(Type messageType)
    {
        return messageType.FullName ?? messageType.Name;
    }

    private static string ToSentenceCase(string value)
    {
        return char.ToUpperInvariant(value[0]) + value.Substring(1);
    }

    private void LogWorstCaseChannelCount(int worstCaseChannelCount, string description)
    {
        _loggerFactory.CreateLogger<RabbitMqInboundTopologyCompiler>().LogDebug(
            "RabbitMQ inbound topology may open up to {ChannelCount} channels ({Description})",
            worstCaseChannelCount,
            description
        );
    }
}
