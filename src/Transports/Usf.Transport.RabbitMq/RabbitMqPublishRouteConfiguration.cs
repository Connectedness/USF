using System;
using System.Collections.Generic;

namespace Usf.Transport.RabbitMq;

public abstract record RabbitMqPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
);

public sealed record RabbitMqFanoutPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
) : RabbitMqPublishRouteConfiguration(MessageType, ExchangeName, TargetName, SerializerType, IsMandatory);

public abstract record RabbitMqRoutingKeyPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqPublishRouteConfiguration(MessageType, ExchangeName, TargetName, SerializerType, IsMandatory);

public sealed record RabbitMqDirectPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqRoutingKeyPublishRouteConfiguration(
    MessageType,
    ExchangeName,
    TargetName,
    SerializerType,
    IsMandatory,
    RoutingKey,
    RoutingKeyFactory
);

public sealed record RabbitMqTopicPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqRoutingKeyPublishRouteConfiguration(
    MessageType,
    ExchangeName,
    TargetName,
    SerializerType,
    IsMandatory,
    RoutingKey,
    RoutingKeyFactory
);

public sealed record RabbitMqHeadersPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    IReadOnlyDictionary<string, object?> Headers
) : RabbitMqPublishRouteConfiguration(MessageType, ExchangeName, TargetName, SerializerType, IsMandatory);
