using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqOutboundTopologyBuilder
{
    private readonly List<RabbitMqAddressDefinition> _addressDefinitions = [];
    private readonly List<RabbitMqBindingDefinition> _bindingDefinitions = [];
    private readonly List<RabbitMqChannelGroupDefinition> _channelGroupDefinitions = [];
    private readonly List<RabbitMqExchangeDefinition> _exchangeDefinitions = [];
    private readonly List<RabbitMqQueueDefinition> _queueDefinitions = [];
    private readonly List<RabbitMqOutboundTargetDefinition> _targets = [];
    private Func<IServiceProvider, ConnectionFactory>? _createConnectionFactory;
    private RabbitMqPublisherConfirmMode _defaultPublisherConfirmMode = RabbitMqPublisherConfirmDefaults.Mode;
    private TimeSpan _defaultPublisherConfirmTimeout = RabbitMqPublisherConfirmDefaults.Timeout;
    private MessageContractRegistryBuilder? _messageContracts;

    /// <summary>
    /// Configures the RabbitMQ connection factory used when the outbound topology first opens a connection.
    /// </summary>
    /// <remarks>
    /// <see cref="ConnectionFactory.AutomaticRecoveryEnabled" /> must be <see langword="true" />.
    /// <see cref="ConnectionFactory.NetworkRecoveryInterval" /> remains caller-configurable.
    /// <see cref="ConnectionFactory.TopologyRecoveryEnabled" /> remains caller-controlled and defaults to
    /// <see langword="true" /> in RabbitMQ.Client. It governs exchange, queue, and binding recovery rather than
    /// connection recovery, and need not be enabled when broker topology is provisioned externally.
    /// </remarks>
    public RabbitMqOutboundTopologyBuilder UseConnectionFactory(ConnectionFactory connectionFactory)
    {
        if (connectionFactory is null)
        {
            throw new ArgumentNullException(nameof(connectionFactory));
        }

        var capturedFactory = connectionFactory;
        _createConnectionFactory = _ => capturedFactory;
        return this;
    }

    /// <summary>
    /// Configures the RabbitMQ connection factory delegate invoked when the outbound topology first opens a connection.
    /// </summary>
    /// <remarks>
    /// <see cref="ConnectionFactory.AutomaticRecoveryEnabled" /> must be <see langword="true" />.
    /// <see cref="ConnectionFactory.NetworkRecoveryInterval" /> remains caller-configurable.
    /// <see cref="ConnectionFactory.TopologyRecoveryEnabled" /> remains caller-controlled and defaults to
    /// <see langword="true" /> in RabbitMQ.Client. It governs exchange, queue, and binding recovery rather than
    /// connection recovery, and need not be enabled when broker topology is provisioned externally.
    /// </remarks>
    public RabbitMqOutboundTopologyBuilder UseConnectionFactory(
        Func<IServiceProvider, ConnectionFactory> createConnectionFactory
    )
    {
        _createConnectionFactory = createConnectionFactory ??
                                   throw new ArgumentNullException(nameof(createConnectionFactory));
        return this;
    }

    public RabbitMqOutboundTopologyBuilder Exchange(
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

    public RabbitMqOutboundTopologyBuilder Queue(string name, Action<RabbitMqQueueBuilder>? configure = null)
    {
        RabbitMqQueueBuilder builder = new (name);
        configure?.Invoke(builder);
        _queueDefinitions.Add(builder.Build());
        return this;
    }

    public RabbitMqOutboundTopologyBuilder QueueBinding(
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

    public RabbitMqOutboundTopologyBuilder ExchangeBinding(
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

    public RabbitMqOutboundTopologyBuilder Address(string name, string exchangeName)
    {
        _addressDefinitions.Add(
            new RabbitMqAddressDefinition(
                RequireText(name, nameof(name)),
                RequireText(exchangeName, nameof(exchangeName))
            )
        );
        return this;
    }

    public RabbitMqOutboundTopologyBuilder WithDefaultPublisherConfirmMode(
        RabbitMqPublisherConfirmMode publisherConfirmMode
    )
    {
        ValidatePublisherConfirmMode(publisherConfirmMode, nameof(publisherConfirmMode));
        _defaultPublisherConfirmMode = publisherConfirmMode;
        return this;
    }

    public RabbitMqOutboundTopologyBuilder WithDefaultPublisherConfirmTimeout(TimeSpan publisherConfirmTimeout)
    {
        ValidatePublisherConfirmTimeout(publisherConfirmTimeout, nameof(publisherConfirmTimeout));
        _defaultPublisherConfirmTimeout = publisherConfirmTimeout;
        return this;
    }

    public RabbitMqOutboundTopologyBuilder MapMessageContracts(Action<MessageContractRegistryBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        _messageContracts ??= new MessageContractRegistryBuilder();
        configure(_messageContracts);
        return this;
    }

    public RabbitMqOutboundTopologyBuilder ChannelGroup(
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

        _channelGroupDefinitions.Add(
            new RabbitMqChannelGroupDefinition(
                channelGroupName,
                maximumChannelCount,
                publisherConfirmMode,
                publisherConfirmTimeout
            )
        );
        return this;
    }

    public RabbitMqOutboundTopologyBuilder Publish<TMessage>(
        Action<RabbitMqOutboundTargetBuilder<TMessage>> configure
    )
    {
        return PublishCore(null, configure);
    }

    public RabbitMqOutboundTopologyBuilder PublishNamed<TMessage>(
        string targetName,
        Action<RabbitMqOutboundTargetBuilder<TMessage>> configure
    )
    {
        return PublishCore(RequireText(targetName, nameof(targetName)), configure);
    }

    private RabbitMqOutboundTopologyBuilder PublishCore<TMessage>(
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

    public RabbitMqOutboundTopologyConfiguration Build()
    {
        return new RabbitMqOutboundTopologyConfiguration(
            _createConnectionFactory,
            _exchangeDefinitions.AsReadOnly(),
            _queueDefinitions.AsReadOnly(),
            _bindingDefinitions.AsReadOnly(),
            _addressDefinitions.AsReadOnly(),
            _channelGroupDefinitions.AsReadOnly(),
            _targets.AsReadOnly(),
            _defaultPublisherConfirmMode,
            _defaultPublisherConfirmTimeout,
            (MessageContractRegistry?) _messageContracts?.Build()
        );
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
