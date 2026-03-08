using System.Collections.Generic;

namespace Usf.Transport.RabbitMq.Configuration;

public sealed record RabbitMqBindingDefinition(
    string ExchangeName,
    string QueueName,
    string RoutingKey,
    RabbitMqDeclareMode DeclareMode,
    IReadOnlyDictionary<string, object?> Arguments
);
