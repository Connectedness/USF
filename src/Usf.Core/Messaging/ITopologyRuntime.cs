using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

/// <summary>
/// A small runtime lifecycle seam for topology instances that have active background work, such as RabbitMQ
/// consumers, NATS subscriptions, SQS polling loops, or Azure Service Bus processors. The topology model itself
/// describes compiled declarations and dispatch definitions; an <see cref="ITopologyRuntime" /> describes the
/// active transport behavior. Publish-only topologies do not register a runtime unless a future transport gains
/// publish-side background work.
/// </summary>
public interface ITopologyRuntime
{
    string TopologyName { get; }

    Task StartAsync(CancellationToken cancellationToken = default);

    Task StopAsync(CancellationToken cancellationToken = default);
}
