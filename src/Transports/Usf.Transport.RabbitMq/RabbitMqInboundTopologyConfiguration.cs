using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed record RabbitMqInboundTopologyConfiguration(
    Func<IServiceProvider, ConnectionFactory>? CreateConnectionFactory,
    IReadOnlyList<RabbitMqExchangeDefinition> Exchanges,
    IReadOnlyList<RabbitMqQueueDefinition> Queues,
    IReadOnlyList<RabbitMqBindingDefinition> Bindings,
    IReadOnlyList<RabbitMqInboundChannelGroupDefinition> ChannelGroups,
    IReadOnlyList<RabbitMqInboundHandlerDefinition> Handlers,
    Type DeserializationMiddlewareType,
    Action<MessagePipelineBuilder>? ConfigurePipeline,
    TimeSpan ShutdownTimeout,
    MessageContractRegistry? MessageContractDialect = null
);
