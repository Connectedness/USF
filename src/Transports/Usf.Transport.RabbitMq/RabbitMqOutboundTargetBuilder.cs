using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqOutboundTargetBuilder<TMessage>
{
    private readonly Dictionary<string, object?> _headers = new (StringComparer.Ordinal);
    private string? _addressName;
    private string? _channelGroupName;
    private string? _routingKey;
    private Func<TMessage, string>? _routingKeyFactory;
    private RabbitMqOutboundRouteScenario _scenario;
    private Type? _serializerType;

    public bool IsMandatory { get; private set; }

    public RabbitMqOutboundTargetBuilder<TMessage> ToFanoutAddress(string addressName)
    {
        _addressName = RequireText(addressName, nameof(addressName));
        _scenario = RabbitMqOutboundRouteScenario.Fanout;
        _routingKey = null;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> ToDirectAddress(string addressName, string routingKey)
    {
        _addressName = RequireText(addressName, nameof(addressName));
        _scenario = RabbitMqOutboundRouteScenario.Direct;
        _routingKey = routingKey ?? string.Empty;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> ToDirectAddress(
        string addressName,
        Func<TMessage, string> routingKeyFactory
    )
    {
        _addressName = RequireText(addressName, nameof(addressName));
        _scenario = RabbitMqOutboundRouteScenario.Direct;
        _routingKey = null;
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
        _headers.Clear();
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> ToTopicAddress(string addressName, string routingKey)
    {
        _addressName = RequireText(addressName, nameof(addressName));
        _scenario = RabbitMqOutboundRouteScenario.Topic;
        _routingKey = routingKey ?? string.Empty;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> ToTopicAddress(
        string addressName,
        Func<TMessage, string> routingKeyFactory
    )
    {
        _addressName = RequireText(addressName, nameof(addressName));
        _scenario = RabbitMqOutboundRouteScenario.Topic;
        _routingKey = null;
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
        _headers.Clear();
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> ToHeadersAddress(string addressName)
    {
        _addressName = RequireText(addressName, nameof(addressName));
        _scenario = RabbitMqOutboundRouteScenario.Headers;
        _routingKey = null;
        _routingKeyFactory = null;
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> UseChannelGroup(string channelGroupName)
    {
        _channelGroupName = RequireText(channelGroupName, nameof(channelGroupName));
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> WithHeader(string name, object? value)
    {
        _headers[RequireText(name, nameof(name))] = value;
        return this;
    }

    public RabbitMqOutboundTargetBuilder<TMessage> WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        _serializerType = typeof(TSerializer);
        return this;
    }

    /// <summary>
    /// Requests a delivery failure when RabbitMQ cannot route a published message to a queue.
    /// </summary>
    /// <remarks>
    /// Mandatory routing requires publisher confirmations on the target's effective channel group so the
    /// returned message can be correlated with its publish. A mandatory target whose effective group uses
    /// <see cref="Configuration.RabbitMqPublisherConfirmMode.FireAndForget" /> is rejected at compile time
    /// through <see cref="Usf.Core.Messaging.Errors.TopologyValidationException" />; select
    /// <see cref="Configuration.RabbitMqPublisherConfirmMode.Confirms" /> on the group (see
    /// <c>RabbitMqTopologyBuilder.ChannelGroup</c>) or leave the topology-level default
    /// (<c>WithDefaultPublisherConfirmMode</c>) on confirms. Confirmation tracking serializes outstanding
    /// publishes per channel while awaiting broker outcomes; increase the channel-group size when relaxed
    /// ordering is acceptable and additional throughput is required.
    /// </remarks>
    public RabbitMqOutboundTargetBuilder<TMessage> Mandatory(bool mandatory = true)
    {
        IsMandatory = mandatory;
        return this;
    }

    internal RabbitMqOutboundTargetDefinition Build(string? targetName)
    {
        if (_addressName is null)
        {
            throw new InvalidOperationException("A RabbitMQ outbound target must select an address.");
        }

        return _scenario switch
        {
            RabbitMqOutboundRouteScenario.Fanout => new RabbitMqFanoutOutboundTargetDefinition(
                typeof(TMessage),
                _addressName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory
            ),
            RabbitMqOutboundRouteScenario.Direct => new RabbitMqDirectOutboundTargetDefinition(
                typeof(TMessage),
                _addressName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory,
                _routingKey,
                _routingKeyFactory
            ),
            RabbitMqOutboundRouteScenario.Topic => new RabbitMqTopicOutboundTargetDefinition(
                typeof(TMessage),
                _addressName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory,
                _routingKey,
                _routingKeyFactory
            ),
            RabbitMqOutboundRouteScenario.Headers => new RabbitMqHeadersOutboundTargetDefinition(
                typeof(TMessage),
                _addressName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory,
                new ReadOnlyDictionary<string, object?>(_headers)
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(_scenario), _scenario, "Unsupported route scenario.")
        };
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
