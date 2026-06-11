using System;
using RabbitMQ.Client;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// The direction-neutral configuration surface shared by outbound and inbound RabbitMQ topology builders:
/// the connection factory, broker resources (exchanges, queues, bindings), and message contract mappings.
/// <see cref="IRabbitMqOutboundTopologyBuilder" /> and <see cref="IRabbitMqInboundTopologyBuilder" /> derive
/// from this interface and add the publish-only and consume-only surfaces, respectively.
/// </summary>
/// <typeparam name="TSelf">The derived builder interface, returned by every member for fluent chaining.</typeparam>
public interface IRabbitMqTopologyBuilder<out TSelf>
    where TSelf : IRabbitMqTopologyBuilder<TSelf>
{
    /// <summary>
    /// Configures the RabbitMQ connection factory used when the topology first opens its connection.
    /// </summary>
    /// <remarks>
    /// <see cref="ConnectionFactory.AutomaticRecoveryEnabled" /> must be <see langword="true" />. When the topology
    /// contains inbound consumers, <see cref="ConnectionFactory.TopologyRecoveryEnabled" /> must also be
    /// <see langword="true" /> so RabbitMQ.Client can recover consumer subscriptions.
    /// </remarks>
    TSelf UseConnectionFactory(ConnectionFactory connectionFactory);

    /// <inheritdoc cref="UseConnectionFactory(ConnectionFactory)" />
    TSelf UseConnectionFactory(Func<IServiceProvider, ConnectionFactory> createConnectionFactory);

    /// <summary>
    /// Declares an exchange that this topology provisions on the broker.
    /// </summary>
    TSelf Exchange(string name, string type, Action<RabbitMqExchangeBuilder>? configure = null);

    /// <summary>
    /// Declares a queue that this topology provisions on the broker.
    /// </summary>
    TSelf Queue(string name, Action<RabbitMqQueueBuilder>? configure = null);

    /// <summary>
    /// Declares a binding from an exchange to a queue that this topology provisions on the broker.
    /// </summary>
    TSelf QueueBinding(
        string exchangeName,
        string queueName,
        string routingKey = "",
        Action<RabbitMqQueueBindingBuilder>? configure = null
    );

    /// <summary>
    /// Declares a binding from a source exchange to a destination exchange that this topology provisions
    /// on the broker.
    /// </summary>
    TSelf ExchangeBinding(
        string sourceExchangeName,
        string destinationExchangeName,
        string routingKey = "",
        Action<RabbitMqExchangeBindingBuilder>? configure = null
    );

    /// <summary>
    /// Maps message contracts for this topology. Mappings configured here take precedence over the
    /// globally registered <see cref="IMessageContractRegistry" />.
    /// </summary>
    TSelf MapMessageContracts(Action<MessageContractRegistryBuilder> configure);
}
