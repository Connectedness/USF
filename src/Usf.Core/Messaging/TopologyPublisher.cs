using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;

namespace Usf.Core.Messaging;

public readonly struct TopologyPublisher
{
    private readonly MessagePublisher _router;
    private readonly TopologyName _topologyName;

    public TopologyPublisher(MessagePublisher router, TopologyName topologyName)
    {
        _router = router ?? throw new System.ArgumentNullException(nameof(router));
        _topologyName = topologyName;
    }

    public Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        return _router.PublishMessageAsync(message, target, _topologyName, cancellationToken);
    }

    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    )
    {
        return _router.PublishMessageAsync(message, in metadata, target, _topologyName, cancellationToken);
    }

    public Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    )
    {
        return _router.PublishRawAsync(message, target, _topologyName, cancellationToken);
    }
}
