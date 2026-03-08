using System.Collections.Generic;

namespace Usf.Transport.RabbitMq.Configuration;

public sealed record RabbitMqQueueDefinition(
    string Name,
    RabbitMqDeclareMode DeclareMode,
    bool Durable,
    bool Exclusive,
    bool AutoDelete,
    IReadOnlyDictionary<string, object?> Arguments
);
