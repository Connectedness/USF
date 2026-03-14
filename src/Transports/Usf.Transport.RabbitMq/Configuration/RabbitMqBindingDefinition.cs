using System.Collections.Generic;

namespace Usf.Transport.RabbitMq.Configuration;

public abstract record RabbitMqBindingDefinition(
    string SourceExchangeName,
    string RoutingKey,
    RabbitMqBindingDeclareMode DeclareMode,
    IReadOnlyDictionary<string, object?> Arguments
);

public sealed record RabbitMqQueueBindingDefinition(
    string SourceExchangeName,
    string QueueName,
    string RoutingKey,
    RabbitMqBindingDeclareMode DeclareMode,
    IReadOnlyDictionary<string, object?> Arguments
) : RabbitMqBindingDefinition(SourceExchangeName, RoutingKey, DeclareMode, Arguments);

public sealed record RabbitMqExchangeBindingDefinition(
    string SourceExchangeName,
    string DestinationExchangeName,
    string RoutingKey,
    RabbitMqBindingDeclareMode DeclareMode,
    IReadOnlyDictionary<string, object?> Arguments
) : RabbitMqBindingDefinition(SourceExchangeName, RoutingKey, DeclareMode, Arguments);
