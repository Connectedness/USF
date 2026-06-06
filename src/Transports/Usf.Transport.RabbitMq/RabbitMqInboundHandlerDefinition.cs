using System;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed record RabbitMqInboundHandlerDefinition(
    string QueueName,
    string? EndpointName,
    Type MessageType,
    Type HandlerType,
    Type SerializerType,
    Type InspectorType,
    string? ChannelGroupName,
    int ChannelCount,
    ushort PrefetchCount,
    ushort ConsumerDispatchConcurrency,
    MessageAckMode AckMode
);
