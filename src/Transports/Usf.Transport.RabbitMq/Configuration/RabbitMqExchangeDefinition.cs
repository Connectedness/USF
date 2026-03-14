using System.Collections.Generic;

namespace Usf.Transport.RabbitMq.Configuration;

public sealed record RabbitMqExchangeDefinition(
    string Name,
    string Type,
    RabbitMqDeclareMode DeclareMode,
    bool Durable,
    bool AutoDelete,
    IReadOnlyDictionary<string, object?> Arguments
);
