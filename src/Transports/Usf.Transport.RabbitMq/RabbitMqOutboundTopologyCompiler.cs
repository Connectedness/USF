using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqOutboundTopologyCompiler
{
    private static readonly MethodInfo CreateTargetMethod = typeof(RabbitMqOutboundTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    public static RabbitMqOutboundTopology Compile(IServiceProvider serviceProvider)
    {
        if (serviceProvider is null)
        {
            throw new ArgumentNullException(nameof(serviceProvider));
        }

        var configuration = serviceProvider.GetRequiredService<RabbitMqOutboundTopologyConfiguration>();
        var validationErrors = Validate(serviceProvider, configuration);

        if (validationErrors.Count > 0)
        {
            throw new OutboundTopologyValidationException(validationErrors);
        }

        RabbitMqOutboundTopology? topology = null;
        var connectionProvider = new RabbitMqConnectionProvider(
            cancellationToken => CreateConnectionAsync(configuration, serviceProvider, cancellationToken)
        );
        var explicitChannelGroupsByName = new Dictionary<string, RabbitMqChannelGroup>(StringComparer.Ordinal);
        List<RabbitMqChannelGroup> channelGroups = [];

        foreach (var channelGroupDefinition in OrderChannelGroups(configuration.ChannelGroups))
        {
            var channelGroup = CreateChannelGroup(channelGroupDefinition, () => topology!);
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
            var channelGroup = ResolveChannelGroup(
                targetDefinition,
                targetName,
                explicitChannelGroupsByName,
                channelGroups,
                () => topology!
            );
            var address = addressesByName[targetDefinition.AddressName];
            var exchangeName = exchangesByName[address.ExchangeName].Name;
            var target = CreateTarget(targetDefinition, serviceProvider, channelGroup, exchangeName);
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

        var (worstCaseChannelCount, description) = RabbitMqOutboundChannelBudget.Calculate(channelGroups);
        LogWorstCaseChannelCount(serviceProvider, worstCaseChannelCount, description);

        topology = new RabbitMqOutboundTopology(
            new OutboundTopology(defaultTargetsByMessageType, targetsByName),
            configuration.Exchanges,
            configuration.Queues,
            configuration.Bindings,
            configuration.Addresses,
            channelGroups.AsReadOnly(),
            targets.AsReadOnly(),
            connectionProvider,
            worstCaseChannelCount,
            description
        );

        return topology;
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

        return connectionFactory.CreateConnectionAsync(cancellationToken);
    }

    private static IEnumerable<RabbitMqChannelGroupDefinition> OrderChannelGroups(
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
        Func<RabbitMqOutboundTopology> getTopology
    )
    {
        return new RabbitMqChannelGroup(
            definition.Name,
            definition.MaximumChannelCount,
            async cancellationToken => await getTopology().CreateChannelAsync(cancellationToken).ConfigureAwait(false)
        );
    }

    private static RabbitMqChannelGroup ResolveChannelGroup(
        RabbitMqOutboundTargetDefinition targetDefinition,
        string targetName,
        IReadOnlyDictionary<string, RabbitMqChannelGroup> explicitChannelGroupsByName,
        ICollection<RabbitMqChannelGroup> channelGroups,
        Func<RabbitMqOutboundTopology> getTopology
    )
    {
        if (!string.IsNullOrWhiteSpace(targetDefinition.ChannelGroupName))
        {
            return explicitChannelGroupsByName[targetDefinition.ChannelGroupName!];
        }

        var implicitChannelGroup = CreateChannelGroup(
            new RabbitMqChannelGroupDefinition($"$implicit:{channelGroups.Count}:{targetName}", 1),
            getTopology
        );
        channelGroups.Add(implicitChannelGroup);
        return implicitChannelGroup;
    }

    private static OutboundTarget CreateTarget(
        RabbitMqOutboundTargetDefinition targetDefinition,
        IServiceProvider serviceProvider,
        RabbitMqChannelGroup channelGroup,
        string exchangeName
    )
    {
        var closedMethod = CreateTargetMethod.MakeGenericMethod(targetDefinition.MessageType);
        return (OutboundTarget) closedMethod.Invoke(
            null,
            [targetDefinition, serviceProvider, channelGroup, exchangeName]
        )!;
    }

    private static OutboundTarget CreateTargetCore<TMessage>(
        RabbitMqOutboundTargetDefinition targetDefinition,
        IServiceProvider serviceProvider,
        RabbitMqChannelGroup channelGroup,
        string exchangeName
    )
    {
        var serializer = (IMessageSerializer) serviceProvider.GetRequiredService(targetDefinition.SerializerType!);
        var targetName = GetTargetName(targetDefinition);

        return targetDefinition switch
        {
            RabbitMqFanoutOutboundTargetDefinition fanoutTarget => new RabbitMqFanoutOutboundTarget<TMessage>(
                targetName,
                serializer,
                channelGroup,
                exchangeName,
                fanoutTarget.IsMandatory
            ),
            RabbitMqDirectOutboundTargetDefinition directTarget => new RabbitMqDirectOutboundTarget<TMessage>(
                targetName,
                serializer,
                channelGroup,
                exchangeName,
                directTarget.IsMandatory,
                directTarget.RoutingKey,
                CreateRoutingKeyFactory<TMessage>(directTarget)
            ),
            RabbitMqTopicOutboundTargetDefinition topicTarget => new RabbitMqTopicOutboundTarget<TMessage>(
                targetName,
                serializer,
                channelGroup,
                exchangeName,
                topicTarget.IsMandatory,
                topicTarget.RoutingKey,
                CreateRoutingKeyFactory<TMessage>(topicTarget)
            ),
            RabbitMqHeadersOutboundTargetDefinition headersTarget => new RabbitMqHeadersOutboundTarget<TMessage>(
                targetName,
                serializer,
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

    private static List<string> Validate(
        IServiceProvider serviceProvider,
        RabbitMqOutboundTopologyConfiguration configuration
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
        validationErrors.AddRange(FindDuplicateNames(configuration.Queues.Select(static queue => queue.Name), "queue"));
        validationErrors.AddRange(
            FindDuplicateNames(configuration.Addresses.Select(static address => address.Name), "address")
        );
        validationErrors.AddRange(
            FindDuplicateNames(configuration.ChannelGroups.Select(static group => group.Name), "channel group")
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
        var channelGroupsByName = ToDictionary(configuration.ChannelGroups, static group => group.Name);

        ValidateExchangeDefinitions(configuration.Exchanges, validationErrors);
        ValidateQueueDefinitions(configuration.Queues, validationErrors);
        ValidateAddressDefinitions(configuration.Addresses, exchangesByName, validationErrors);
        ValidateChannelGroupDefinitions(configuration.ChannelGroups, validationErrors);
        ValidateChannelGroupUsage(configuration.ChannelGroups, configuration.Targets, validationErrors);
        ValidateTargets(
            serviceProvider,
            configuration.Targets,
            addressesByName,
            exchangesByName,
            channelGroupsByName,
            validationErrors
        );
        ValidateBindings(configuration.Bindings, exchangesByName, queuesByName, validationErrors);

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

    private static void ValidateChannelGroupDefinitions(
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
        }
    }

    private static void ValidateChannelGroupUsage(
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

    private static void ValidateTargets(
        IServiceProvider serviceProvider,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        IReadOnlyDictionary<string, RabbitMqAddressDefinition> addressesByName,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqChannelGroupDefinition> channelGroupsByName,
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
                    serviceProvider,
                    target,
                    addressesByName,
                    exchangesByName,
                    channelGroupsByName,
                    validationErrors
                );
            }
        }
    }

    private static void ValidateTarget(
        IServiceProvider serviceProvider,
        RabbitMqOutboundTargetDefinition target,
        IReadOnlyDictionary<string, RabbitMqAddressDefinition> addressesByName,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqChannelGroupDefinition> channelGroupsByName,
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
        else if (serviceProvider.GetService(target.SerializerType) is null)
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

    private static void LogWorstCaseChannelCount(
        IServiceProvider serviceProvider,
        int worstCaseChannelCount,
        string description
    )
    {
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger(typeof(RabbitMqOutboundTopologyCompiler));
        logger.LogInformation(
            "RabbitMQ outbound topology may open up to {ChannelCount} channels ({Description}).",
            worstCaseChannelCount,
            description
        );
    }
}
