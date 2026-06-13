using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;

namespace Usf.Core.Messaging;

public interface IMessagePublisher
{
    TopologyPublisher ForTopology(TopologyName topologyName);

    Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent;

    Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    );

    Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    );
}
