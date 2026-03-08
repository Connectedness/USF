using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

internal static class RabbitMqMessageTopologyCompiler
{
    private static readonly MethodInfo CreateTargetMethod = typeof(RabbitMqMessageTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    public static MessageTopology Compile(IServiceProvider serviceProvider)
    {
        if (serviceProvider is null)
        {
            throw new ArgumentNullException(nameof(serviceProvider));
        }

        var configuration = serviceProvider.GetRequiredService<RabbitMqPublishingConfiguration>();
        var validationErrors = Validate(serviceProvider, configuration);

        if (validationErrors.Count > 0)
        {
            throw new MessageTopologyValidationException(validationErrors);
        }

        var connectionManager = serviceProvider.GetRequiredService<RabbitMqConnectionManager>();
        Dictionary<Type, Target> targetsByMessageType = new ();
        Dictionary<string, Target> targetsByName = new (StringComparer.Ordinal);

        foreach (var route in configuration.Routes)
        {
            var target = CreateTarget(route, serviceProvider, connectionManager);
            targetsByMessageType.Add(route.MessageType, target);

            if (!string.IsNullOrWhiteSpace(route.TargetName))
            {
                targetsByName.Add(route.TargetName!, target);
            }
        }

        return new MessageTopology(targetsByMessageType, targetsByName);
    }

    private static Target CreateTarget(
        RabbitMqPublishRouteConfiguration route,
        IServiceProvider serviceProvider,
        RabbitMqConnectionManager connectionManager
    )
    {
        var closedMethod = CreateTargetMethod.MakeGenericMethod(route.MessageType);
        return (Target) closedMethod.Invoke(null, [route, serviceProvider, connectionManager])!;
    }

    private static Target CreateTargetCore<TMessage>(
        RabbitMqPublishRouteConfiguration route,
        IServiceProvider serviceProvider,
        RabbitMqConnectionManager connectionManager
    )
    {
        var serializer = (IMessageSerializer) serviceProvider.GetRequiredService(route.SerializerType!);
        var targetName = string.IsNullOrWhiteSpace(route.TargetName) ?
            route.MessageType.FullName ?? route.MessageType.Name :
            route.TargetName!;

        return new RabbitMqTarget<TMessage>(
            targetName,
            serializer,
            connectionManager,
            route.ExchangeName!,
            route.RoutingKey,
            route.IsMandatory
        );
    }

    private static List<string> Validate(
        IServiceProvider serviceProvider,
        RabbitMqPublishingConfiguration configuration
    )
    {
        List<string> validationErrors = [];

        if (configuration.ConnectionFactoryFactory is null)
        {
            validationErrors.Add("A RabbitMQ connection factory must be configured.");
        }

        validationErrors.AddRange(
            FindDuplicateNames(configuration.Exchanges.Select(static exchange => exchange.Name), "exchange")
        );
        validationErrors.AddRange(FindDuplicateNames(configuration.Queues.Select(static queue => queue.Name), "queue"));
        validationErrors.AddRange(
            FindDuplicateNames(
                configuration.Routes.Where(static route => !string.IsNullOrWhiteSpace(route.TargetName))
                   .Select(static route => route.TargetName!),
                "target"
            )
        );
        validationErrors.AddRange(
            FindDuplicateNames(
                configuration.Routes.Select(static route => route.MessageType.AssemblyQualifiedName!),
                "message route"
            )
        );

        var exchangesByName = configuration.Exchanges
           .GroupBy(static exchange => exchange.Name, StringComparer.Ordinal)
           .Select(static group => group.First())
           .ToDictionary(static exchange => exchange.Name, StringComparer.Ordinal);
        var queuesByName = configuration.Queues
           .GroupBy(static queue => queue.Name, StringComparer.Ordinal)
           .Select(static group => group.First())
           .ToDictionary(static queue => queue.Name, StringComparer.Ordinal);

        foreach (var route in configuration.Routes.OrderBy(
                     static route => route.MessageType.AssemblyQualifiedName,
                     StringComparer.Ordinal
                 ))
        {
            var messageTypeName = route.MessageType.FullName ?? route.MessageType.Name;

            if (string.IsNullOrWhiteSpace(route.ExchangeName))
            {
                validationErrors.Add($"Message route '{messageTypeName}' must reference a RabbitMQ exchange.");
            }
            else if (!exchangesByName.ContainsKey(route.ExchangeName!))
            {
                validationErrors.Add(
                    $"Message route '{messageTypeName}' references unknown exchange '{route.ExchangeName}'."
                );
            }

            if (route.SerializerType is null)
            {
                validationErrors.Add($"Message route '{messageTypeName}' must configure a serializer.");
            }
            else if (!typeof(IMessageSerializer).IsAssignableFrom(route.SerializerType))
            {
                validationErrors.Add(
                    $"Serializer '{route.SerializerType}' for message route '{messageTypeName}' does not implement '{typeof(IMessageSerializer)}'."
                );
            }
            else if (serviceProvider.GetService(route.SerializerType) is null)
            {
                validationErrors.Add(
                    $"Serializer '{route.SerializerType}' for message route '{messageTypeName}' is not registered in the service provider."
                );
            }
        }

        foreach (var binding in configuration.Bindings
                    .OrderBy(static binding => binding.ExchangeName, StringComparer.Ordinal)
                    .ThenBy(static binding => binding.QueueName, StringComparer.Ordinal).ThenBy(
                         static binding => binding.RoutingKey,
                         StringComparer.Ordinal
                     ))
        {
            if (!exchangesByName.TryGetValue(binding.ExchangeName, out var exchange))
            {
                validationErrors.Add(
                    $"Binding from exchange '{binding.ExchangeName}' to queue '{binding.QueueName}' references an unknown exchange."
                );
                continue;
            }

            if (!queuesByName.TryGetValue(binding.QueueName, out var queue))
            {
                validationErrors.Add(
                    $"Binding from exchange '{binding.ExchangeName}' to queue '{binding.QueueName}' references an unknown queue."
                );
                continue;
            }

            if (binding.DeclareMode != RabbitMqDeclareMode.None &&
                (exchange.DeclareMode == RabbitMqDeclareMode.None || queue.DeclareMode == RabbitMqDeclareMode.None))
            {
                validationErrors.Add(
                    $"Binding from exchange '{binding.ExchangeName}' to queue '{binding.QueueName}' cannot use declare mode '{binding.DeclareMode}' when either referenced entity uses '{RabbitMqDeclareMode.None}'."
                );
            }
        }

        return validationErrors;
    }

    private static IEnumerable<string> FindDuplicateNames(IEnumerable<string> names, string entityDescription)
    {
        return names
           .GroupBy(static name => name, StringComparer.Ordinal)
           .Where(static group => group.Count() > 1)
           .Select(group => $"Duplicate {entityDescription} '{group.Key}' is configured.");
    }
}
