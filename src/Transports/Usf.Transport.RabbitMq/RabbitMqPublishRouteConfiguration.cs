using System;

namespace Usf.Transport.RabbitMq;

internal sealed record RabbitMqPublishRouteConfiguration(
    Type MessageType,
    string? ExchangeName,
    string RoutingKey,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
);
