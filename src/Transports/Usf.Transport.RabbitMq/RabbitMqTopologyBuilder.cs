using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// Configures a single RabbitMQ topology. The builder exposes the shared broker-resource surface
/// (<see cref="UseConnectionFactory(ConnectionFactory)" />, <see cref="Exchange" />, <see cref="Queue" />,
/// <see cref="QueueBinding" />, <see cref="ExchangeBinding" />, <see cref="MapMessageContracts" />), outbound
/// publishing configuration (<see cref="Address" />, <see cref="Publish{TMessage}" />,
/// <see cref="PublishNamed{TMessage}" />, the outbound <see cref="ChannelGroup(string,int,RabbitMqPublisherConfirmMode?,TimeSpan?)" />
/// overload, and publisher-confirm defaults), and inbound consumer configuration
/// (<see cref="Consume" />, the inbound <see cref="ChannelGroup(string,int,ushort,ushort)" /> overload,
/// <see cref="ConfigureInboundPipeline" />, <see cref="UseDeserializationMiddleware{TMiddleware}" />, and
/// <see cref="WithShutdownTimeout" />).
/// </summary>
public sealed class RabbitMqTopologyBuilder
{
    private readonly List<RabbitMqAddressDefinition> _addressDefinitions = [];
    private readonly List<RabbitMqBindingDefinition> _bindingDefinitions = [];
    private readonly List<RabbitMqExchangeDefinition> _exchangeDefinitions = [];
    private readonly List<RabbitMqInboundHandlerDefinition> _handlers = [];

    private readonly List<RabbitMqInboundChannelGroupDefinition> _inboundChannelGroupDefinitions = [];
    private readonly List<RabbitMqChannelGroupDefinition> _outboundChannelGroupDefinitions = [];
    private readonly List<RabbitMqQueueDefinition> _queueDefinitions = [];
    private readonly List<RabbitMqOutboundTargetDefinition> _targets = [];

    private Action<MessagePipelineBuilder>? _configurePipeline;
    private Func<IServiceProvider, ConnectionFactory>? _createConnectionFactory;
    private RabbitMqPublisherConfirmMode _defaultPublisherConfirmMode = RabbitMqPublisherConfirmDefaults.Mode;
    private TimeSpan _defaultPublisherConfirmTimeout = RabbitMqPublisherConfirmDefaults.Timeout;
    private Type _deserializationMiddlewareType = typeof(MessageDeserializationMiddleware);
    private MessageContractRegistryBuilder? _messageContracts;
    private TimeSpan _shutdownTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Configures the RabbitMQ connection factory used when the topology first opens its connection.
    /// </summary>
    /// <remarks>
    /// <see cref="ConnectionFactory.AutomaticRecoveryEnabled" /> must be <see langword="true" />. When the topology
    /// contains inbound consumers, <see cref="ConnectionFactory.TopologyRecoveryEnabled" /> must also be
    /// <see langword="true" /> so RabbitMQ.Client can recover consumer subscriptions.
    /// </remarks>
    public RabbitMqTopologyBuilder UseConnectionFactory(ConnectionFactory connectionFactory)
    {
        if (connectionFactory is null)
        {
            throw new ArgumentNullException(nameof(connectionFactory));
        }

        var capturedFactory = connectionFactory;
        _createConnectionFactory = _ => capturedFactory;
        return this;
    }

    /// <inheritdoc cref="UseConnectionFactory(ConnectionFactory)" />
    public RabbitMqTopologyBuilder UseConnectionFactory(
        Func<IServiceProvider, ConnectionFactory> createConnectionFactory
    )
    {
        _createConnectionFactory = createConnectionFactory ??
                                   throw new ArgumentNullException(nameof(createConnectionFactory));
        return this;
    }

    public RabbitMqTopologyBuilder Exchange(
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

    public RabbitMqTopologyBuilder Queue(string name, Action<RabbitMqQueueBuilder>? configure = null)
    {
        RabbitMqQueueBuilder builder = new (name);
        configure?.Invoke(builder);
        _queueDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqTopologyBuilder QueueBinding(
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

    public RabbitMqTopologyBuilder ExchangeBinding(
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

    public RabbitMqTopologyBuilder MapMessageContracts(Action<MessageContractRegistryBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        _messageContracts ??= new MessageContractRegistryBuilder();
        configure(_messageContracts);
        return this;
    }

    public RabbitMqTopologyBuilder Address(string name, string exchangeName)
    {
        _addressDefinitions.Add(
            new RabbitMqAddressDefinition(
                RequireText(name, nameof(name)),
                RequireText(exchangeName, nameof(exchangeName))
            )
        );
        return this;
    }

    public RabbitMqTopologyBuilder WithDefaultPublisherConfirmMode(
        RabbitMqPublisherConfirmMode publisherConfirmMode
    )
    {
        ValidatePublisherConfirmMode(publisherConfirmMode, nameof(publisherConfirmMode));
        _defaultPublisherConfirmMode = publisherConfirmMode;
        return this;
    }

    public RabbitMqTopologyBuilder WithDefaultPublisherConfirmTimeout(TimeSpan publisherConfirmTimeout)
    {
        ValidatePublisherConfirmTimeout(publisherConfirmTimeout, nameof(publisherConfirmTimeout));
        _defaultPublisherConfirmTimeout = publisherConfirmTimeout;
        return this;
    }

    /// <summary>
    /// Configures an outbound publisher channel group.
    /// </summary>
    public RabbitMqTopologyBuilder ChannelGroup(
        string name,
        int maximumChannelCount,
        RabbitMqPublisherConfirmMode? publisherConfirmMode = null,
        TimeSpan? publisherConfirmTimeout = null
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

        if (publisherConfirmMode is not null)
        {
            ValidatePublisherConfirmMode(publisherConfirmMode.Value, nameof(publisherConfirmMode));
        }

        if (publisherConfirmTimeout is not null)
        {
            ValidatePublisherConfirmTimeout(publisherConfirmTimeout.Value, nameof(publisherConfirmTimeout));
        }

        var channelGroupName = RequireText(name, nameof(name));

        if (channelGroupName.StartsWith(
                RabbitMqChannelGroupDefinition.ReservedImplicitNamePrefix,
                StringComparison.Ordinal
            ))
        {
            throw new ArgumentException(
                $"Channel group names beginning with '{RabbitMqChannelGroupDefinition.ReservedImplicitNamePrefix}' are reserved.",
                nameof(name)
            );
        }

        _outboundChannelGroupDefinitions.Add(
            new RabbitMqChannelGroupDefinition(
                channelGroupName,
                maximumChannelCount,
                publisherConfirmMode,
                publisherConfirmTimeout
            )
        );
        return this;
    }

    /// <summary>
    /// Configures an inbound consumer channel group.
    /// </summary>
    public RabbitMqTopologyBuilder ChannelGroup(
        string name,
        int maximumChannelCount,
        ushort prefetchCount,
        ushort consumerDispatchConcurrency
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

        _inboundChannelGroupDefinitions.Add(
            new RabbitMqInboundChannelGroupDefinition(
                channelGroupName,
                maximumChannelCount,
                prefetchCount,
                consumerDispatchConcurrency
            )
        );
        return this;
    }

    public RabbitMqTopologyBuilder Publish<TMessage>(
        Action<RabbitMqOutboundTargetBuilder<TMessage>> configure
    )
    {
        return PublishCore(null, configure);
    }

    public RabbitMqTopologyBuilder PublishNamed<TMessage>(
        string targetName,
        Action<RabbitMqOutboundTargetBuilder<TMessage>> configure
    )
    {
        return PublishCore(RequireText(targetName, nameof(targetName)), configure);
    }

    public RabbitMqTopologyBuilder Consume(
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

    public RabbitMqTopologyBuilder ConfigureInboundPipeline(Action<MessagePipelineBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        _configurePipeline += configure;
        return this;
    }

    public RabbitMqTopologyBuilder UseDeserializationMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware
    {
        _deserializationMiddlewareType = typeof(TMiddleware);
        return this;
    }

    public RabbitMqTopologyBuilder WithShutdownTimeout(TimeSpan shutdownTimeout)
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

    public RabbitMqTopologyConfiguration Build()
    {
        return new RabbitMqTopologyConfiguration(
            _createConnectionFactory,
            _exchangeDefinitions.AsReadOnly(),
            _queueDefinitions.AsReadOnly(),
            _bindingDefinitions.AsReadOnly(),
            _addressDefinitions.AsReadOnly(),
            _outboundChannelGroupDefinitions.AsReadOnly(),
            _targets.AsReadOnly(),
            _inboundChannelGroupDefinitions.AsReadOnly(),
            _handlers.AsReadOnly(),
            _deserializationMiddlewareType,
            _configurePipeline,
            _shutdownTimeout,
            _defaultPublisherConfirmMode,
            _defaultPublisherConfirmTimeout,
            (MessageContractRegistry?) _messageContracts?.Build()
        );
    }

    private RabbitMqTopologyBuilder PublishCore<TMessage>(
        string? targetName,
        Action<RabbitMqOutboundTargetBuilder<TMessage>> configure
    )
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        RabbitMqOutboundTargetBuilder<TMessage> builder = new ();
        configure(builder);
        _targets.Add(builder.Build(targetName));
        return this;
    }

    private static void ValidatePublisherConfirmMode(
        RabbitMqPublisherConfirmMode publisherConfirmMode,
        string parameterName
    )
    {
        if (!Enum.IsDefined(typeof(RabbitMqPublisherConfirmMode), publisherConfirmMode))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                publisherConfirmMode,
                "Unsupported publisher confirm mode."
            );
        }
    }

    private static void ValidatePublisherConfirmTimeout(TimeSpan publisherConfirmTimeout, string parameterName)
    {
        if (!RabbitMqPublisherConfirmDefaults.IsValidTimeout(publisherConfirmTimeout))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                publisherConfirmTimeout,
                "The value must be finite and greater than zero."
            );
        }
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
