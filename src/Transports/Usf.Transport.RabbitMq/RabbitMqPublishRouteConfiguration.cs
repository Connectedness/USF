using System;
using System.Collections.Generic;

namespace Usf.Transport.RabbitMq;

internal abstract record RabbitMqPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
);

internal sealed record RabbitMqFanoutPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
) : RabbitMqPublishRouteConfiguration(MessageType, ExchangeName, TargetName, SerializerType, IsMandatory);

internal abstract record RabbitMqRoutingKeyPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqPublishRouteConfiguration(MessageType, ExchangeName, TargetName, SerializerType, IsMandatory);

internal sealed record RabbitMqDirectPublishRouteConfiguration(
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

internal sealed record RabbitMqTopicPublishRouteConfiguration(
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

internal sealed record RabbitMqHeadersPublishRouteConfiguration(
    Type MessageType,
    string ExchangeName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    IReadOnlyDictionary<string, object?> Headers
) : RabbitMqPublishRouteConfiguration(MessageType, ExchangeName, TargetName, SerializerType, IsMandatory);
