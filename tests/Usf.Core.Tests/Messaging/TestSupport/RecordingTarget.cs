using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class RecordingTarget<TMessage> : OutboundTarget<TMessage>
{
    public RecordingTarget(string name, IMessageSerializer serializer)
        : this(name, serializer, CloudEventsTestFactory.CreateRegistry()) { }

    public RecordingTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName? topologyName = null
    )
        : base(name, "test", serializer, messageContractRegistry, topologyName) { }

    public List<TMessage> Messages { get; } = [];

    public List<CloudEventEnvelope> CloudEventEnvelopes { get; } = [];

    public List<string?> RoutingKeys { get; } = [];

    public List<SerializedMessage> SerializedMessages { get; } = [];

    public override Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    )
    {
        SerializedMessages.Add(message);
        return Task.CompletedTask;
    }

    protected override Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message);
        CloudEventEnvelopes.Add(envelope);
        RoutingKeys.Add(routingKey);
        return Task.CompletedTask;
    }
}
