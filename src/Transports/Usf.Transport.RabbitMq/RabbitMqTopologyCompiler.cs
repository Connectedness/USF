using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// Validates a <see cref="RabbitMqTopologyConfiguration" /> and compiles it into a single
/// <see cref="RabbitMqTopology" /> that owns one connection and exposes both outbound targets and inbound
/// endpoints through the Core <see cref="Topology" /> base.
/// </summary>
public sealed class RabbitMqTopologyCompiler
{
    private static readonly MethodInfo CreateTargetMethod = typeof(RabbitMqTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo CreateEndpointMethod = typeof(RabbitMqTopologyCompiler)
       .GetMethod(nameof(CreateEndpointCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private readonly IMessageContractRegistry _canonicalMessageContracts;
    private readonly Func<Type, bool> _isServiceRegistered;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Func<Type, IMessageSerializer?> _resolveSerializer;

    public RabbitMqTopologyCompiler(
        IMessageContractRegistry canonicalMessageContracts,
        ILoggerFactory loggerFactory,
        Func<Type, IMessageSerializer?> resolveSerializer,
        Func<Type, bool> isServiceRegistered
    )
    {
        _canonicalMessageContracts = canonicalMessageContracts ??
                                     throw new ArgumentNullException(nameof(canonicalMessageContracts));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _resolveSerializer = resolveSerializer ?? throw new ArgumentNullException(nameof(resolveSerializer));
        _isServiceRegistered = isServiceRegistered ?? throw new ArgumentNullException(nameof(isServiceRegistered));
    }

    public RabbitMqTopology Compile(
        string topologyName,
        RabbitMqTopologyConfiguration configuration,
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
            throw new TopologyValidationException(validationErrors);
        }

        RabbitMqChannelSource channelSource = new (connectionProvider);

        var (outboundChannelGroups, targets, defaultTargetsByMessageType, targetsByName) = CompileOutbound(
            topologyName,
            configuration,
            effectiveMessageContracts,
            channelSource
        );
        var (inboundChannelGroups, endpoints, endpointsByName, dispatchIndex) = CompileInbound(
            topologyName,
            configuration,
            effectiveMessageContracts
        );

        var (worstCaseChannelCount, description) = CalculateChannelBudget(
            outboundChannelGroups,
            inboundChannelGroups
        );
        channelSource.SetChannelBudget(worstCaseChannelCount, description);
        LogWorstCaseChannelCount(worstCaseChannelCount, description);
        var topology = new RabbitMqTopology(
            topologyName,
            TopologyData.PrepareTopologyDataStructures(
                defaultTargetsByMessageType,
                targetsByName,
                endpointsByName
            ),
            effectiveMessageContracts,
            configuration.Exchanges,
            configuration.Queues,
            configuration.Bindings,
            configuration.Addresses,
            outboundChannelGroups.AsReadOnly(),
            targets.AsReadOnly(),
            inboundChannelGroups.AsReadOnly(),
            endpoints.AsReadOnly(),
            new ReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint>(dispatchIndex),
            BuildPipeline(configuration),
            configuration.ShutdownTimeout,
            connectionProvider,
            channelSource
        );
        WarnWhenEmpty(topology);
        return topology;
    }

    private IMessageContractRegistry CreateEffectiveMessageContracts(RabbitMqTopologyConfiguration configuration)
    {
        return configuration.MessageContractDialect is null ?
            _canonicalMessageContracts :
            new EffectiveMessageContractRegistry(_canonicalMessageContracts, configuration.MessageContractDialect);
    }

    // ----- Outbound compilation -----

    private (
        List<RabbitMqChannelGroup> ChannelGroups,
        List<OutboundTarget> Targets,
        Dictionary<Type, OutboundTarget> DefaultTargetsByMessageType,
        Dictionary<string, OutboundTarget> TargetsByName
        ) CompileOutbound(
            string topologyName,
            RabbitMqTopologyConfiguration configuration,
            IMessageContractRegistry effectiveMessageContracts,
            RabbitMqChannelSource channelSource
        )
    {
        Dictionary<string, RabbitMqChannelGroup> explicitChannelGroupsByName = new (StringComparer.Ordinal);
        List<RabbitMqChannelGroup> channelGroups = [];

        foreach (var channelGroupDefinition in OrderOutboundChannelGroups(configuration.OutboundChannelGroups))
        {
            var channelGroup = CreateChannelGroup(
                channelGroupDefinition,
                configuration.DefaultPublisherConfirmMode,
                configuration.DefaultPublisherConfirmTimeout,
                channelSource
            );
            explicitChannelGroupsByName.Add(channelGroup.Name, channelGroup);
            channelGroups.Add(channelGroup);
        }

        var exchangesByName = ToDictionary(configuration.Exchanges, static exchange => exchange.Name);
        var addressesByName = ToDictionary(configuration.Addresses, static address => address.Name);
        Dictionary<Type, OutboundTarget> defaultTargetsByMessageType = new ();
        Dictionary<string, OutboundTarget> targetsByName = new (StringComparer.Ordinal);
        List<OutboundTarget> targets = [];

        foreach (var targetDefinition in OrderTargets(configuration.Targets))
        {
            var targetName = GetTargetName(targetDefinition);
            var channelGroup = ResolveOutboundChannelGroup(
                targetDefinition,
                targetName,
                explicitChannelGroupsByName,
                channelGroups,
                configuration.DefaultPublisherConfirmMode,
                configuration.DefaultPublisherConfirmTimeout,
                channelSource
            );
            var address = addressesByName[targetDefinition.AddressName];
            var exchangeName = exchangesByName[address.ExchangeName].Name;
            var target = CreateTarget(
                targetDefinition,
                topologyName,
                effectiveMessageContracts,
                channelGroup,
                exchangeName
            );
            targets.Add(target);

            if (string.IsNullOrWhiteSpace(targetDefinition.TargetName))
            {
                defaultTargetsByMessageType.Add(targetDefinition.MessageType, target);
            }
            else
            {
                targetsByName.Add(targetDefinition.TargetName!, target);
            }
        }

        return (channelGroups, targets, defaultTargetsByMessageType, targetsByName);
    }

    private static IEnumerable<RabbitMqChannelGroupDefinition> OrderOutboundChannelGroups(
        IReadOnlyList<RabbitMqChannelGroupDefinition> channelGroups
    )
    {
        return channelGroups.OrderBy(static channelGroup => channelGroup.Name, StringComparer.Ordinal);
    }

    private static IEnumerable<RabbitMqOutboundTargetDefinition> OrderTargets(
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets
    )
    {
        return targets
           .OrderBy(static target => target.MessageType.AssemblyQualifiedName, StringComparer.Ordinal)
           .ThenBy(static target => target.TargetName ?? string.Empty, StringComparer.Ordinal);
    }

    private static RabbitMqChannelGroup CreateChannelGroup(
        RabbitMqChannelGroupDefinition definition,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        TimeSpan? defaultPublisherConfirmTimeout,
        RabbitMqChannelSource channelSource
    )
    {
        var publisherConfirmMode = definition.PublisherConfirmMode ?? defaultPublisherConfirmMode;

        return new RabbitMqChannelGroup(
            definition.Name,
            definition.MaximumChannelCount,
            async cancellationToken => await channelSource
               .CreateChannelAsync(CreateChannelOptions(publisherConfirmMode), cancellationToken)
               .ConfigureAwait(false),
            publisherConfirmMode,
            definition.PublisherConfirmTimeout ?? defaultPublisherConfirmTimeout
        );
    }

    private static CreateChannelOptions? CreateChannelOptions(RabbitMqPublisherConfirmMode publisherConfirmMode)
    {
        return publisherConfirmMode switch
        {
            RabbitMqPublisherConfirmMode.FireAndForget => null,
            RabbitMqPublisherConfirmMode.Confirms => new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true
            ),
            _ => throw new ArgumentOutOfRangeException(
                nameof(publisherConfirmMode),
                publisherConfirmMode,
                "Unsupported publisher confirm mode."
            )
        };
    }

    private static RabbitMqChannelGroup ResolveOutboundChannelGroup(
        RabbitMqOutboundTargetDefinition targetDefinition,
        string targetName,
        IReadOnlyDictionary<string, RabbitMqChannelGroup> explicitChannelGroupsByName,
        ICollection<RabbitMqChannelGroup> channelGroups,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        TimeSpan? defaultPublisherConfirmTimeout,
        RabbitMqChannelSource channelSource
    )
    {
        if (!string.IsNullOrWhiteSpace(targetDefinition.ChannelGroupName))
        {
            return explicitChannelGroupsByName[targetDefinition.ChannelGroupName!];
        }

        var implicitChannelGroup = CreateChannelGroup(
            new RabbitMqChannelGroupDefinition(
                $"{RabbitMqChannelGroupDefinition.ReservedImplicitNamePrefix}{channelGroups.Count}:{targetName}",
                1
            ),
            defaultPublisherConfirmMode,
            defaultPublisherConfirmTimeout,
            channelSource
        );
        channelGroups.Add(implicitChannelGroup);
        return implicitChannelGroup;
    }

    private OutboundTarget CreateTarget(
        RabbitMqOutboundTargetDefinition targetDefinition,
        string topologyName,
        IMessageContractRegistry messageContractRegistry,
        RabbitMqChannelGroup channelGroup,
        string exchangeName
    )
    {
        var serializer = _resolveSerializer(targetDefinition.SerializerType!) ??
                         throw new InvalidOperationException(
                             $"Serializer '{targetDefinition.SerializerType}' is not registered."
                         );
        var closedMethod = CreateTargetMethod.MakeGenericMethod(targetDefinition.MessageType);
        return (OutboundTarget) closedMethod.Invoke(
            null,
            [targetDefinition, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName]
        )!;
    }

    private static OutboundTarget CreateTargetCore<TMessage>(
        RabbitMqOutboundTargetDefinition targetDefinition,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqChannelGroup channelGroup,
        string exchangeName
    )
    {
        var targetName = GetTargetName(targetDefinition);

        return targetDefinition switch
        {
            RabbitMqFanoutOutboundTargetDefinition fanoutTarget => new RabbitMqFanoutOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                fanoutTarget.IsMandatory
            ),
            RabbitMqDirectOutboundTargetDefinition directTarget => new RabbitMqDirectOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                directTarget.IsMandatory,
                directTarget.RoutingKey,
                CreateRoutingKeyFactory<TMessage>(directTarget)
            ),
            RabbitMqTopicOutboundTargetDefinition topicTarget => new RabbitMqTopicOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                topicTarget.IsMandatory,
                topicTarget.RoutingKey,
                CreateRoutingKeyFactory<TMessage>(topicTarget)
            ),
            RabbitMqHeadersOutboundTargetDefinition headersTarget => new RabbitMqHeadersOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                headersTarget.IsMandatory,
                headersTarget.Headers
            ),
            _ => throw new ArgumentOutOfRangeException(
                nameof(targetDefinition),
                targetDefinition,
                "Unsupported RabbitMQ outbound target."
            )
        };
    }

    private static Func<TMessage, string>? CreateRoutingKeyFactory<TMessage>(
        RabbitMqRoutingKeyOutboundTargetDefinition targetDefinition
    )
    {
        if (targetDefinition.RoutingKeyFactory is null)
        {
            return null;
        }

        if (targetDefinition.RoutingKeyFactory is Func<TMessage, string> typedRoutingKeyFactory)
        {
            return typedRoutingKeyFactory;
        }

        throw new ArgumentException("A routing-key target must provide a routing-key factory for its message type.");
    }

    // ----- Inbound compilation -----

    private (
        List<RabbitMqInboundChannelGroup> ChannelGroups,
        List<RabbitMqInboundEndpoint> Endpoints,
        Dictionary<string, InboundEndpoint> EndpointsByName,
        Dictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> DispatchIndex
        ) CompileInbound(
            string topologyName,
            RabbitMqTopologyConfiguration configuration,
            IMessageContractRegistry effectiveMessageContracts
        )
    {
        Dictionary<string, RabbitMqInboundChannelGroup> explicitChannelGroupsByName = new (StringComparer.Ordinal);
        List<RabbitMqInboundChannelGroup> channelGroups = [];
        Dictionary<string, InboundEndpoint> endpointsByName = new (StringComparer.Ordinal);
        Dictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> dispatchIndex =
            new (InboundEndpointSelectionKeyComparer.Instance);
        List<RabbitMqInboundEndpoint> endpoints = [];

        foreach (var channelGroupDefinition in OrderInboundChannelGroups(configuration.InboundChannelGroups))
        {
            var channelGroup = CreateInboundChannelGroup(channelGroupDefinition);
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
            var channelGroup = ResolveInboundChannelGroup(
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
            }
        }

        return (channelGroups, endpoints, endpointsByName, dispatchIndex);
    }

    private static IEnumerable<RabbitMqInboundChannelGroupDefinition> OrderInboundChannelGroups(
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

    private static RabbitMqInboundChannelGroup CreateInboundChannelGroup(
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

    private static RabbitMqInboundChannelGroup ResolveInboundChannelGroup(
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
        string topologyName,
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
        string topologyName,
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
            handlerDefinition.HandlerInvocation,
            handlerDefinition.AckMode,
            handlerDefinition.QueueName,
            handlerDefinition.InspectorType,
            channelGroup
        );
    }

    private MessageDelegate BuildPipeline(RabbitMqTopologyConfiguration configuration)
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
        return pipeline.Build(static context => context.Endpoint.InvokeHandlerAsync(context));
    }

    // ----- Channel budget -----

    private static (int WorstCaseChannelCount, string Description) CalculateChannelBudget(
        IReadOnlyList<RabbitMqChannelGroup> outboundChannelGroups,
        IReadOnlyList<RabbitMqInboundChannelGroup> inboundChannelGroups
    )
    {
        List<(string Name, int MaximumChannelCount)> all = [];
        all.AddRange(outboundChannelGroups.Select(static group => (group.Name, group.MaximumChannelCount)));
        all.AddRange(inboundChannelGroups.Select(static group => (group.Name, group.MaximumChannelCount)));

        if (all.Count == 0)
        {
            return (0, "no channel groups configured");
        }

        var worstCaseChannelCount = all.Sum(static group => group.MaximumChannelCount);

        if (all.Count == 1)
        {
            return (worstCaseChannelCount, $"channel group '{all[0].Name}' max {all[0].MaximumChannelCount}");
        }

        return (worstCaseChannelCount, $"{all.Count} channel groups");
    }

    // ----- Validation -----

    private List<string> Validate(
        RabbitMqTopologyConfiguration configuration,
        IMessageContractRegistry effectiveMessageContracts
    )
    {
        List<string> validationErrors = [];

        if (configuration.CreateConnectionFactory is null)
        {
            validationErrors.Add("A RabbitMQ connection factory must be configured.");
        }

        validationErrors.AddRange(
            FindDuplicateNames(configuration.Exchanges.Select(static exchange => exchange.Name), "exchange")
        );
        validationErrors.AddRange(
            FindDuplicateNames(configuration.Queues.Select(static queue => queue.Name), "queue")
        );
        validationErrors.AddRange(
            FindDuplicateNames(configuration.Addresses.Select(static address => address.Name), "address")
        );
        validationErrors.AddRange(
            FindDuplicateNames(configuration.OutboundChannelGroups.Select(static group => group.Name), "channel group")
        );
        validationErrors.AddRange(
            FindDuplicateNames(
                configuration.Targets.Where(static target => !string.IsNullOrWhiteSpace(target.TargetName))
                   .Select(static target => target.TargetName!),
                "target"
            )
        );

        var exchangesByName = ToDictionary(configuration.Exchanges, static exchange => exchange.Name);
        var queuesByName = ToDictionary(configuration.Queues, static queue => queue.Name);
        var addressesByName = ToDictionary(configuration.Addresses, static address => address.Name);
        var outboundChannelGroupsByName = ToDictionary(
            configuration.OutboundChannelGroups,
            static group => group.Name
        );
        var inboundChannelGroupsByName = ToDictionary(
            configuration.InboundChannelGroups,
            static group => group.Name
        );

        ValidateExchangeDefinitions(configuration.Exchanges, validationErrors);
        ValidateQueueDefinitions(configuration.Queues, validationErrors);
        ValidateBindings(configuration.Bindings, exchangesByName, queuesByName, validationErrors);

        // Outbound validation.
        ValidateAddressDefinitions(configuration.Addresses, exchangesByName, validationErrors);
        ValidateDefaultPublisherConfirmConfiguration(configuration, validationErrors);
        ValidateOutboundChannelGroupDefinitions(configuration.OutboundChannelGroups, validationErrors);
        ValidateOutboundChannelGroupUsage(configuration.OutboundChannelGroups, configuration.Targets, validationErrors);
        ValidateTargets(
            configuration.Targets,
            addressesByName,
            exchangesByName,
            outboundChannelGroupsByName,
            configuration.DefaultPublisherConfirmMode,
            validationErrors
        );
        ValidateMessageContracts(effectiveMessageContracts, configuration.Targets, validationErrors);
        ValidateMessageContractDialect(configuration.MessageContractDialect, configuration.Targets, validationErrors);

        // Inbound validation.
        ValidateInboundChannelGroupDefinitions(configuration.InboundChannelGroups, validationErrors);
        ValidateInboundChannelGroupUsage(configuration.InboundChannelGroups, configuration.Handlers, validationErrors);
        ValidatePipeline(configuration, validationErrors);
        ValidateHandlers(
            configuration.Handlers,
            queuesByName,
            inboundChannelGroupsByName,
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
                validationErrors.Add($"Exchange '{exchange.Name}' uses unsupported exchange type 'internal'.");
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
                validationErrors.Add($"Queue '{queue.Name}' uses unsupported declare mode '{queue.DeclareMode}'.");
            }
        }
    }

    private static void ValidateAddressDefinitions(
        IReadOnlyList<RabbitMqAddressDefinition> addresses,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        ICollection<string> validationErrors
    )
    {
        foreach (var address in addresses.OrderBy(static address => address.Name, StringComparer.Ordinal))
        {
            if (!exchangesByName.ContainsKey(address.ExchangeName))
            {
                validationErrors.Add(
                    $"Address '{address.Name}' references unknown exchange '{address.ExchangeName}'."
                );
            }
        }
    }

    private static void ValidateOutboundChannelGroupDefinitions(
        IReadOnlyList<RabbitMqChannelGroupDefinition> channelGroups,
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

            if (channelGroup.Name.StartsWith(
                    RabbitMqChannelGroupDefinition.ReservedImplicitNamePrefix,
                    StringComparison.Ordinal
                ))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' uses reserved name prefix '{RabbitMqChannelGroupDefinition.ReservedImplicitNamePrefix}'."
                );
            }

            if (channelGroup.PublisherConfirmMode is not null &&
                !Enum.IsDefined(typeof(RabbitMqPublisherConfirmMode), channelGroup.PublisherConfirmMode.Value))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' uses unsupported publisher confirm mode '{channelGroup.PublisherConfirmMode}'."
                );
            }

            if (channelGroup.PublisherConfirmTimeout is not null &&
                !RabbitMqPublisherConfirmDefaults.IsValidTimeout(channelGroup.PublisherConfirmTimeout.Value))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' publisher confirm timeout must be finite and greater than zero."
                );
            }
        }
    }

    private static void ValidateDefaultPublisherConfirmConfiguration(
        RabbitMqTopologyConfiguration configuration,
        ICollection<string> validationErrors
    )
    {
        if (!Enum.IsDefined(typeof(RabbitMqPublisherConfirmMode), configuration.DefaultPublisherConfirmMode))
        {
            validationErrors.Add(
                $"RabbitMQ outbound topology uses unsupported default publisher confirm mode '{configuration.DefaultPublisherConfirmMode}'."
            );
        }

        if (configuration.DefaultPublisherConfirmTimeout is not null &&
            !RabbitMqPublisherConfirmDefaults.IsValidTimeout(configuration.DefaultPublisherConfirmTimeout.Value))
        {
            validationErrors.Add(
                "RabbitMQ outbound topology publisher confirm timeout must be finite and greater than zero."
            );
        }
    }

    private static void ValidateOutboundChannelGroupUsage(
        IReadOnlyList<RabbitMqChannelGroupDefinition> channelGroups,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        ICollection<string> validationErrors
    )
    {
        var referencedChannelGroups = new HashSet<string>(
            targets
               .Where(static target => !string.IsNullOrWhiteSpace(target.ChannelGroupName))
               .Select(static target => target.ChannelGroupName!),
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
                    $"Channel group '{channelGroupName}' is configured but no outbound target references it."
                );
            }
        }
    }

    private void ValidateTargets(
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        IReadOnlyDictionary<string, RabbitMqAddressDefinition> addressesByName,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqChannelGroupDefinition> channelGroupsByName,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        ICollection<string> validationErrors
    )
    {
        foreach (var group in targets.GroupBy(
                         static target => target.MessageType.AssemblyQualifiedName!,
                         StringComparer.Ordinal
                     )
                    .OrderBy(static group => group.Key, StringComparer.Ordinal))
        {
            var unnamedTargetCount = group.Count(static target => string.IsNullOrWhiteSpace(target.TargetName));
            var messageType = group.First().MessageType;
            var messageTypeName = messageType.FullName ?? messageType.Name;

            if (unnamedTargetCount > 1)
            {
                validationErrors.Add(
                    $"Message '{messageTypeName}' configures multiple default RabbitMQ outbound targets."
                );
            }

            foreach (var target in group
                        .OrderBy(static target => target.TargetName ?? string.Empty, StringComparer.Ordinal))
            {
                ValidateTarget(
                    target,
                    addressesByName,
                    exchangesByName,
                    channelGroupsByName,
                    defaultPublisherConfirmMode,
                    validationErrors
                );
            }
        }
    }

    private void ValidateTarget(
        RabbitMqOutboundTargetDefinition target,
        IReadOnlyDictionary<string, RabbitMqAddressDefinition> addressesByName,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqChannelGroupDefinition> channelGroupsByName,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        ICollection<string> validationErrors
    )
    {
        var targetDescription = GetTargetDescription(target);

        if (!addressesByName.TryGetValue(target.AddressName, out var address))
        {
            validationErrors.Add($"{targetDescription} references unknown address '{target.AddressName}'.");
        }
        else if (exchangesByName.TryGetValue(address.ExchangeName, out var exchange))
        {
            ValidateTargetAgainstExchange(target, exchange, targetDescription, validationErrors);
        }

        if (!string.IsNullOrWhiteSpace(target.ChannelGroupName) &&
            !channelGroupsByName.ContainsKey(target.ChannelGroupName!))
        {
            validationErrors.Add(
                $"{targetDescription} references unknown channel group '{target.ChannelGroupName}'."
            );
        }

        if (target.IsMandatory &&
            TryGetPublisherConfirmMode(
                target,
                channelGroupsByName,
                defaultPublisherConfirmMode,
                out var publisherConfirmMode
            ) &&
            publisherConfirmMode == RabbitMqPublisherConfirmMode.FireAndForget)
        {
            validationErrors.Add(
                $"{targetDescription} enables mandatory routing but its effective channel group uses fire-and-forget publishing."
            );
        }

        if (target.SerializerType is null)
        {
            validationErrors.Add($"{targetDescription} must configure a serializer.");
        }
        else if (!typeof(IMessageSerializer).IsAssignableFrom(target.SerializerType))
        {
            validationErrors.Add(
                $"Serializer '{target.SerializerType}' for {targetDescription.ToLowerInvariant()} does not implement '{typeof(IMessageSerializer)}'."
            );
        }
        else if (_resolveSerializer(target.SerializerType) is null)
        {
            validationErrors.Add(
                $"Serializer '{target.SerializerType}' for {targetDescription.ToLowerInvariant()} is not registered in the service provider."
            );
        }

        if (target is RabbitMqRoutingKeyOutboundTargetDefinition routingKeyTarget)
        {
            ValidateRoutingKeyConfiguration(routingKeyTarget, targetDescription, validationErrors);
        }
    }

    private static bool TryGetPublisherConfirmMode(
        RabbitMqOutboundTargetDefinition target,
        IReadOnlyDictionary<string, RabbitMqChannelGroupDefinition> channelGroupsByName,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        out RabbitMqPublisherConfirmMode publisherConfirmMode
    )
    {
        if (string.IsNullOrWhiteSpace(target.ChannelGroupName))
        {
            publisherConfirmMode = defaultPublisherConfirmMode;
            return true;
        }

        if (channelGroupsByName.TryGetValue(target.ChannelGroupName!, out var channelGroup))
        {
            publisherConfirmMode = channelGroup.PublisherConfirmMode ?? defaultPublisherConfirmMode;
            return true;
        }

        publisherConfirmMode = default;
        return false;
    }

    private static void ValidateTargetAgainstExchange(
        RabbitMqOutboundTargetDefinition target,
        RabbitMqExchangeDefinition exchange,
        string targetDescription,
        ICollection<string> validationErrors
    )
    {
        var expectedExchangeType = target switch
        {
            RabbitMqFanoutOutboundTargetDefinition => ExchangeType.Fanout,
            RabbitMqDirectOutboundTargetDefinition => ExchangeType.Direct,
            RabbitMqTopicOutboundTargetDefinition => ExchangeType.Topic,
            RabbitMqHeadersOutboundTargetDefinition => ExchangeType.Headers,
            _ => string.Empty
        };

        if (!string.Equals(exchange.Type, expectedExchangeType, StringComparison.Ordinal))
        {
            validationErrors.Add(
                $"{targetDescription} targets exchange '{exchange.Name}' of type '{exchange.Type}', but requires '{expectedExchangeType}'."
            );
        }
    }

    private static void ValidateRoutingKeyConfiguration(
        RabbitMqRoutingKeyOutboundTargetDefinition target,
        string targetDescription,
        ICollection<string> validationErrors
    )
    {
        var hasRoutingKey = target.RoutingKey is not null;
        var hasRoutingKeyFactory = target.RoutingKeyFactory is not null;

        if (hasRoutingKey == hasRoutingKeyFactory)
        {
            validationErrors.Add(
                $"{targetDescription} must configure either a constant routing key or a routing-key factory."
            );
        }
    }

    private static void ValidateMessageContracts(
        IMessageContractRegistry registry,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        ICollection<string> validationErrors
    )
    {
        OutboundTargetContractValidator.CollectValidationErrors(
            registry,
            targets.Select(
                static target => new KeyValuePair<string, Type>(GetTargetName(target), target.MessageType)
            ),
            validationErrors
        );
    }

    private static void ValidateMessageContractDialect(
        MessageContractRegistry? dialect,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        ICollection<string> validationErrors
    )
    {
        if (dialect is null)
        {
            return;
        }

        var targetMessageTypes = targets.Select(static target => target.MessageType).ToArray();

        foreach (var messageType in dialect.RegisteredMessageTypes)
        {
            if (targetMessageTypes.Any(targetMessageType => targetMessageType.IsAssignableFrom(messageType)))
            {
                continue;
            }

            validationErrors.Add(
                $"RabbitMQ outbound message-contract dialect maps message type '{messageType}', but no outbound target publishes that type on this topology."
            );
        }
    }

    private static void ValidateInboundChannelGroupDefinitions(
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

    private static void ValidateInboundChannelGroupUsage(
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
        if (!_isServiceRegistered(handler.HandlerType))
        {
            validationErrors.Add(
                $"Inbound handler '{handler.HandlerType}' for message '{GetTypeName(handler.MessageType)}' is not registered."
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
        RabbitMqTopologyConfiguration configuration,
        ICollection<string> validationErrors
    )
    {
        if (configuration.Handlers.Count == 0)
        {
            return;
        }

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
                    .ThenBy(static binding => GetBindingDestinationName(binding), StringComparer.Ordinal)
                    .ThenBy(static binding => binding.RoutingKey, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqBindingMode), binding.BindingMode))
            {
                validationErrors.Add(
                    $"{GetBindingDescription(binding)} uses unsupported binding mode '{binding.BindingMode}'."
                );
            }

            if (!exchangesByName.ContainsKey(binding.SourceExchangeName))
            {
                validationErrors.Add(
                    $"{GetBindingDescription(binding)} references unknown source exchange '{binding.SourceExchangeName}'."
                );
            }

            switch (binding)
            {
                case RabbitMqQueueBindingDefinition queueBinding when !queuesByName.ContainsKey(queueBinding.QueueName):
                    validationErrors.Add(
                        $"{GetBindingDescription(queueBinding)} references unknown queue '{queueBinding.QueueName}'."
                    );
                    break;

                case RabbitMqExchangeBindingDefinition exchangeBinding
                    when !exchangesByName.ContainsKey(exchangeBinding.DestinationExchangeName):
                    validationErrors.Add(
                        $"{GetBindingDescription(exchangeBinding)} references unknown destination exchange '{exchangeBinding.DestinationExchangeName}'."
                    );
                    break;
            }
        }
    }

    private static Dictionary<string, T> ToDictionary<T>(IEnumerable<T> values, Func<T, string> getName)
    {
        return values
           .GroupBy(getName, StringComparer.Ordinal)
           .Select(static group => group.First())
           .ToDictionary(getName, StringComparer.Ordinal);
    }

    private static IEnumerable<string> FindDuplicateNames(IEnumerable<string> names, string entityDescription)
    {
        return names
           .GroupBy(static name => name, StringComparer.Ordinal)
           .Where(static group => group.Count() > 1)
           .Select(group => $"Duplicate {entityDescription} '{group.Key}' is configured.");
    }

    private static string GetTargetName(RabbitMqOutboundTargetDefinition target)
    {
        return string.IsNullOrWhiteSpace(target.TargetName) ?
            target.MessageType.FullName ?? target.MessageType.Name :
            target.TargetName!;
    }

    private static string GetTargetDescription(RabbitMqOutboundTargetDefinition target)
    {
        var messageTypeName = target.MessageType.FullName ?? target.MessageType.Name;

        return string.IsNullOrWhiteSpace(target.TargetName) ?
            $"Outbound target for message '{messageTypeName}'" :
            $"Outbound target for message '{messageTypeName}' and target '{target.TargetName}'";
    }

    private static string GetBindingDescription(RabbitMqBindingDefinition binding)
    {
        return binding switch
        {
            RabbitMqQueueBindingDefinition queueBinding =>
                $"Queue binding from exchange '{queueBinding.SourceExchangeName}' to queue '{queueBinding.QueueName}'",
            RabbitMqExchangeBindingDefinition exchangeBinding =>
                $"Exchange binding from exchange '{exchangeBinding.SourceExchangeName}' to exchange '{exchangeBinding.DestinationExchangeName}'",
            _ => "RabbitMQ binding"
        };
    }

    private static string GetBindingDestinationName(RabbitMqBindingDefinition binding)
    {
        return binding switch
        {
            RabbitMqQueueBindingDefinition queueBinding => queueBinding.QueueName,
            RabbitMqExchangeBindingDefinition exchangeBinding => exchangeBinding.DestinationExchangeName,
            _ => string.Empty
        };
    }

    private static string GetTypeName(Type messageType)
    {
        return messageType.FullName ?? messageType.Name;
    }

    private void LogWorstCaseChannelCount(int worstCaseChannelCount, string description)
    {
        var logger = _loggerFactory.CreateLogger(typeof(RabbitMqTopologyCompiler));
        logger.LogInformation(
            "RabbitMQ topology may open up to {ChannelCount} channels ({Description})",
            worstCaseChannelCount,
            description
        );
    }

    private void WarnWhenEmpty(RabbitMqTopology topology)
    {
        if (!topology.IsEmpty)
        {
            return;
        }

        _loggerFactory.CreateLogger(typeof(RabbitMqTopologyCompiler)).LogWarning(
            "RabbitMQ topology '{TopologyName}' is empty: it declares no outbound targets and no inbound endpoints",
            topology.Name
        );
    }
}
