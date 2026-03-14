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

    public RabbitMqMessagePublishingBuilder QueueBinding(
        string exchangeName,
        string queueName,
        string routingKey = "",
        Action<RabbitMqQueueBindingBuilder>? configure = null
    )
    {
        RabbitMqQueueBindingBuilder builder = new (exchangeName, queueName, routingKey);
        configure?.Invoke(builder);
        _bindingDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqMessagePublishingBuilder ExchangeBinding(
        string sourceExchangeName,
        string destinationExchangeName,
        string routingKey = "",
        Action<RabbitMqExchangeBindingBuilder>? configure = null
    )
    {
        RabbitMqExchangeBindingBuilder builder = new (sourceExchangeName, destinationExchangeName, routingKey);
        configure?.Invoke(builder);
        _bindingDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqMessagePublishingBuilder Publish<TMessage>(Action<RabbitMqPublishRouteBuilder<TMessage>> configure)
    {
        return PublishCore(null, configure);
    }

    public RabbitMqMessagePublishingBuilder PublishNamed<TMessage>(
        string targetName,
        Action<RabbitMqPublishRouteBuilder<TMessage>> configure
    )
    {
        return PublishCore(RequireText(targetName, nameof(targetName)), configure);
    }

    private RabbitMqMessagePublishingBuilder PublishCore<TMessage>(
        string? targetName,
        Action<RabbitMqPublishRouteBuilder<TMessage>> configure
    )
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        RabbitMqPublishRouteBuilder<TMessage> builder = new ();
        configure(builder);
        _routes.Add(builder.Build(targetName));
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

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
