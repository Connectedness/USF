using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundTopologyBuilder
{
    private readonly List<RabbitMqBindingDefinition> _bindingDefinitions = [];
    private readonly List<RabbitMqInboundChannelGroupDefinition> _channelGroupDefinitions = [];
    private readonly List<RabbitMqExchangeDefinition> _exchangeDefinitions = [];
    private readonly List<RabbitMqInboundHandlerDefinition> _handlers = [];
    private readonly List<RabbitMqQueueDefinition> _queueDefinitions = [];
    private Action<MessagePipelineBuilder>? _configurePipeline;
    private Func<IServiceProvider, ConnectionFactory>? _createConnectionFactory;
    private Type _deserializationMiddlewareType = typeof(MessageDeserializationMiddleware);
    private MessageContractRegistryBuilder? _messageContracts;
    private TimeSpan _shutdownTimeout = TimeSpan.FromSeconds(30);

    public RabbitMqInboundTopologyBuilder UseConnectionFactory(ConnectionFactory connectionFactory)
    {
        if (connectionFactory is null)
        {
            throw new ArgumentNullException(nameof(connectionFactory));
        }

        var capturedFactory = connectionFactory;
        _createConnectionFactory = _ => capturedFactory;
        return this;
    }

    public RabbitMqInboundTopologyBuilder UseConnectionFactory(
        Func<IServiceProvider, ConnectionFactory> createConnectionFactory
    )
    {
        _createConnectionFactory = createConnectionFactory ??
                                   throw new ArgumentNullException(nameof(createConnectionFactory));
        return this;
    }

    public RabbitMqInboundTopologyBuilder Exchange(
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

    public RabbitMqInboundTopologyBuilder Queue(string name, Action<RabbitMqQueueBuilder>? configure = null)
    {
        RabbitMqQueueBuilder builder = new (name);
        configure?.Invoke(builder);
        _queueDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqInboundTopologyBuilder QueueBinding(
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

    public RabbitMqInboundTopologyBuilder ExchangeBinding(
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

    public RabbitMqInboundTopologyBuilder MapMessageContracts(Action<MessageContractRegistryBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        _messageContracts ??= new MessageContractRegistryBuilder();
        configure(_messageContracts);
        return this;
    }

    public RabbitMqInboundTopologyBuilder ConfigureInboundPipeline(Action<MessagePipelineBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        _configurePipeline += configure;
        return this;
    }

    public RabbitMqInboundTopologyBuilder UseDeserializationMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware
    {
        _deserializationMiddlewareType = typeof(TMiddleware);
        return this;
    }

    public RabbitMqInboundTopologyBuilder ChannelGroup(
        string name,
        int maximumChannelCount,
        ushort prefetchCount = 1,
        ushort consumerDispatchConcurrency = 1
    )
    {
        if (maximumChannelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumChannelCount),
                maximumChannelCount,
                "The value must be greater than zero."
            );
        }

        if (prefetchCount == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prefetchCount),
                prefetchCount,
                "The value must be greater than zero."
            );
        }

        if (consumerDispatchConcurrency == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerDispatchConcurrency),
                consumerDispatchConcurrency,
                "The value must be greater than zero."
            );
        }

        var channelGroupName = RequireText(name, nameof(name));

        if (channelGroupName.StartsWith(
                RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix,
                StringComparison.Ordinal
            ))
        {
            throw new ArgumentException(
                $"Channel group names beginning with '{RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix}' are reserved.",
                nameof(name)
            );
        }

        _channelGroupDefinitions.Add(
            new RabbitMqInboundChannelGroupDefinition(
                channelGroupName,
                maximumChannelCount,
                prefetchCount,
                consumerDispatchConcurrency
            )
        );
        return this;
    }

    public RabbitMqInboundTopologyBuilder Consume(
        string queueName,
        Action<RabbitMqInboundEndpointBuilder> configure
    )
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        RabbitMqInboundEndpointBuilder builder = new (queueName);
        configure(builder);
        _handlers.AddRange(builder.Build());
        return this;
    }

    public RabbitMqInboundTopologyBuilder WithShutdownTimeout(TimeSpan shutdownTimeout)
    {
        if (shutdownTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shutdownTimeout),
                shutdownTimeout,
                "The value must be greater than zero."
            );
        }

        _shutdownTimeout = shutdownTimeout;
        return this;
    }

    public RabbitMqInboundTopologyConfiguration Build()
    {
        return new RabbitMqInboundTopologyConfiguration(
            _createConnectionFactory,
            _exchangeDefinitions.AsReadOnly(),
            _queueDefinitions.AsReadOnly(),
            _bindingDefinitions.AsReadOnly(),
            _channelGroupDefinitions.AsReadOnly(),
            _handlers.AsReadOnly(),
            _deserializationMiddlewareType,
            _configurePipeline,
            _shutdownTimeout,
            (MessageContractRegistry?) _messageContracts?.Build()
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
