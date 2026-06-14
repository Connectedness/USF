using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// The compiled-from configuration of a single RabbitMQ topology. A topology owns one broker connection and can
/// carry both outbound publishing targets and inbound consumers. Outbound-only or consume-only topologies simply
/// leave the corresponding collections empty.
/// </summary>
public sealed record RabbitMqTopologyConfiguration(
    Func<IServiceProvider, ConnectionFactory>? CreateConnectionFactory,
    IReadOnlyList<RabbitMqExchangeDefinition> Exchanges,
    IReadOnlyList<RabbitMqQueueDefinition> Queues,
    IReadOnlyList<RabbitMqBindingDefinition> Bindings,
    IReadOnlyList<RabbitMqAddressDefinition> Addresses,
    IReadOnlyList<RabbitMqChannelGroupDefinition> OutboundChannelGroups,
    IReadOnlyList<RabbitMqOutboundTargetDefinition> Targets,
    IReadOnlyList<RabbitMqInboundChannelGroupDefinition> InboundChannelGroups,
    IReadOnlyList<RabbitMqInboundHandlerDefinition> Handlers,
    Type DeserializationMiddlewareType,
    Action<MessagePipelineBuilder>? ConfigurePipeline,
    TimeSpan ShutdownTimeout,
    RabbitMqPublisherConfirmMode DefaultPublisherConfirmMode = RabbitMqPublisherConfirmDefaults.Mode,
    TimeSpan? DefaultPublisherConfirmTimeout = null,
    MessageContractRegistry? MessageContractDialect = null
)
{
    public bool HasInboundEndpoints => Handlers.Count > 0;
}
