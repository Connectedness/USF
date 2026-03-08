using System.Collections.Generic;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed record RabbitMqExchangeDefinition(
    string Name,
    string Type,
    RabbitMqDeclareMode DeclareMode,
    bool Durable,
    bool AutoDelete,
    IReadOnlyDictionary<string, object?> Arguments
);
