using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class ThrowingTarget<TMessage> : OutboundTarget<TMessage>
{
    private readonly Exception _exception;

    public ThrowingTarget(string name, IMessageSerializer serializer, Exception exception)
        : this(name, serializer, CloudEventsTestFactory.CreateRegistry(), exception) { }

    public ThrowingTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        Exception exception,
        TopologyName? topologyName = null
    )
        : base(name, "test", serializer, messageContractRegistry, topologyName)
    {
        _exception = exception ?? throw new ArgumentNullException(nameof(exception));
    }

    public override Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    )
    {
        return Task.FromException(_exception);
    }

    protected override Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        return Task.FromException(_exception);
    }
}
