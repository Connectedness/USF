using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqMessagePublishingBuilder
{
    private readonly List<RabbitMqBindingDefinition> _bindingDefinitions = [];
    private readonly List<RabbitMqExchangeDefinition> _exchangeDefinitions = [];
    private readonly List<RabbitMqQueueDefinition> _queueDefinitions = [];
    private readonly List<RabbitMqPublishRouteConfiguration> _routes = [];
    private Func<IServiceProvider, ConnectionFactory>? _connectionFactoryFactory;

    public RabbitMqMessagePublishingBuilder UseConnectionFactory(ConnectionFactory connectionFactory)
    {
        if (connectionFactory is null)
        {
            throw new ArgumentNullException(nameof(connectionFactory));
        }

        var capturedFactory = connectionFactory;
        _connectionFactoryFactory = _ => capturedFactory;
        return this;
    }

    public RabbitMqMessagePublishingBuilder UseConnectionFactory(
        Func<IServiceProvider, ConnectionFactory> connectionFactoryFactory
    )
    {
        _connectionFactoryFactory = connectionFactoryFactory ??
                                    throw new ArgumentNullException(nameof(connectionFactoryFactory));
        return this;
    }

    public RabbitMqMessagePublishingBuilder Exchange(
        string name,
        string type,
        Action<RabbitMqExchangeBuilder>? configure = null
    )
    {
        RabbitMqExchangeBuilder builder = new (name, type);
        configure?.Invoke(builder);
        _exchangeDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqMessagePublishingBuilder Queue(string name, Action<RabbitMqQueueBuilder>? configure = null)
    {
        RabbitMqQueueBuilder builder = new (name);
        configure?.Invoke(builder);
        _queueDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqMessagePublishingBuilder Binding(
        string exchangeName,
        string queueName,
        string routingKey = "",
        Action<RabbitMqBindingBuilder>? configure = null
    )
    {
        RabbitMqBindingBuilder builder = new (exchangeName, queueName, routingKey);
        configure?.Invoke(builder);
        _bindingDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqMessagePublishingBuilder Publish<TMessage>(Action<RabbitMqPublishRouteBuilder<TMessage>> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        RabbitMqPublishRouteBuilder<TMessage> builder = new ();
        configure(builder);
        _routes.Add(builder.Build());
        return this;
    }

    internal RabbitMqPublishingConfiguration Build()
    {
        return new RabbitMqPublishingConfiguration(
            _connectionFactoryFactory,
            _exchangeDefinitions.AsReadOnly(),
            _queueDefinitions.AsReadOnly(),
            _bindingDefinitions.AsReadOnly(),
            _routes.AsReadOnly()
        );
    }
}
