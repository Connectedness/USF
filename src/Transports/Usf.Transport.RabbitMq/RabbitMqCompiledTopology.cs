using System;
using System.Collections.Generic;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

internal sealed class RabbitMqCompiledTopology
{
    public RabbitMqCompiledTopology(
        MessageTopology messageTopology,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings
    )
    {
        MessageTopology = messageTopology ?? throw new ArgumentNullException(nameof(messageTopology));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
    }

    public MessageTopology MessageTopology { get; }

    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }
}
