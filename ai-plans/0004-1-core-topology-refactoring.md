## Rationale

Message consumers introduced inbound topology as a first-class concept, but the inbound/outbound topology split leaked a RabbitMQ-specific operational recommendation into `Usf.Core`. RabbitMQ benefits from separate publisher and consumer connections, but Core should model a topology as one service-owned transport boundary: one recoverable broker connection/client, a set of outbound targets, and a set of inbound endpoint definitions. Transports that need publish/consume isolation can register multiple topology instances; transports that naturally publish and consume through one client can register one topology with both outbound targets and inbound endpoints.

This refactor collapses the Core topology namespace, registry, and default-service model while preserving the programming asymmetry: publishers resolve outbound targets by topology, name, or message type; inbound endpoints are framework-owned runtime definitions and are not directly dispatched by application call sites.

Breaking changes are welcome here, we have not published the library yet.

## Acceptance Criteria

- [ ] `Usf.Core` exposes a single topology abstraction and implementation that contain outbound targets and inbound endpoint definitions.
- [ ] `OutboundTarget` and `InboundEndpoint` remain independent entry types with no shared entry interface or forced behavioral common base.
- [ ] A topology can list all outbound targets and can resolve one outbound target by name or associated message type.
- [ ] A topology can list inbound endpoint definitions and can resolve one endpoint by name for diagnostics, tests, and management use.
- [ ] The public Core registry/catalog model is collapsed from separate inbound and outbound topology namespaces into one topology namespace keyed by `TopologyName`.
- [ ] Publishing APIs resolve the default topology as the default publish topology; consuming-only topologies are started through topology runtime services and are not required to be resolved by normal publish call sites.
- [ ] Direction-specific Core topology infrastructure is removed rather than retained as compatibility aliases, including inbound/outbound topology interfaces, registries, catalogs, single-topology registries, and provisioner aliases.
- [ ] `IOutboundTargetRegistry` is removed; named outbound-target lookup lives directly on `ITopology`.
- [ ] Direction-specific topology validation exceptions are collapsed into one topology validation exception.
- [ ] Core exposes an `ITopologyRuntime` abstraction for topology instances that have active background work and need host-lifetime start/stop behavior.
- [ ] A shared hosted service discovers registered `ITopologyRuntime` instances, starts them after topology provisioning, and stops them gracefully during host shutdown.
- [ ] Core does not introduce a connection-provider abstraction; broker connection/client ownership and recovery remain transport-specific implementation details.
- [ ] RabbitMQ registration supports one topology instance owning exactly one `RabbitMqConnectionProvider`; users can register separate RabbitMQ topology instances when they want separate publisher and consumer connections.
- [ ] RabbitMQ exposes a unified topology builder surface that can configure shared broker resources, outbound publishing targets, inbound consumers, and direction-specific channel/runtime options.
- [ ] Provisioning and runtime startup continue to run in deterministic hosted-service order: broker resources are provisioned before any topology runtime starts.
- [ ] Existing outbound publishing behavior and existing inbound consumer behavior are preserved after the Core topology refactor.
- [ ] Automated tests are updated or added for unified topology registration, duplicate topology-name rejection, default publish topology resolution, consuming-only topology startup, RabbitMQ separate publish/consume connections, and one topology containing both outbound targets and inbound endpoints.

## Technical Details

Core should replace the separate `IOutboundTopology`/`IInboundTopology` public model with one topology surface. A concrete sketch is:

```csharp
using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public interface ITopology
{
    TopologyName TopologyName { get; }

    IReadOnlyCollection<OutboundTarget> OutboundTargets { get; }

    IReadOnlyCollection<InboundEndpoint> InboundEndpoints { get; }

    OutboundTarget GetRequiredTarget(Type messageType);

    OutboundTarget<T> GetRequiredTarget<T>();

    bool TryGetTarget(Type messageType, out OutboundTarget? target);

    OutboundTarget GetRequiredTarget(string name);

    OutboundTarget<T> GetRequiredTarget<T>(string name);

    bool TryGetTarget(string name, out OutboundTarget? target);

    InboundEndpoint GetRequiredEndpoint(string name);

    bool TryGetEndpoint(string name, out InboundEndpoint? endpoint);
}
```

The exact class names can be adjusted during implementation, but the resulting Core model should have one immutable `Topology` aggregate with separate dictionaries for outbound targets by name, outbound targets by message type, and inbound endpoints by name. The current generic `Topology<TEntry>` storage helper can either be removed or kept as an internal implementation detail, but the public concept should be one topology.

`OutboundTarget` and `InboundEndpoint` should stay independent. Outbound targets are executable routes selected by application publishing code. Inbound endpoints are framework-owned handler definitions selected by the inbound runtime after transport inspection. The unified topology should therefore expose symmetric listing and diagnostic lookup, but not imply symmetric dispatch behavior.

The registry layer should collapse to one `TopologyRegistrationCatalog`, one `ITopologyRegistry`, one concrete `TopologyRegistry`, and one `SingleTopologyRegistry` for direct construction tests. Topology names become one namespace. Registering the same topology name twice should fail even if one registration is publish-only and the other is consume-only. This makes topology name match the one-connection/client ownership boundary.

The following Core topology-infrastructure types should be removed, not kept as aliases: `IOutboundTopology`, `IInboundTopology`, `OutboundTopology`, `InboundTopology`, `IOutboundTargetRegistry`, `IOutboundTopologyRegistry`, `IInboundTopologyRegistry`, `OutboundTopologyRegistry`, `InboundTopologyRegistry`, `SingleOutboundTopologyRegistry`, `SingleInboundTopologyRegistry`, `OutboundTopologyRegistrationCatalog`, `InboundTopologyRegistrationCatalog`, `IOutboundTopologyProvisioner`, and `OutboundTopologyHostedService`. `TopologyProvisioningHostedService` and `ITopologyProvisioner` remain as the unified provisioning model.

The direction-specific topology validation exceptions should be collapsed. `OutboundTopologyValidationException` and `InboundTopologyValidationException` should be replaced by a single `TopologyValidationException`. The exception type should be unified, but validation messages must remain precise and should name the failed transport feature and direction where relevant, such as "RabbitMQ outbound target ...", "RabbitMQ inbound endpoint ...", "RabbitMQ exchange ...", or "RabbitMQ consumer channel group ...". `MessageContractOutboundTopologyValidator` should be renamed or reshaped so it validates outbound target message contracts without implying a separate outbound topology kind.

`MessagePublisher` should depend on `ITopologyRegistry` instead of `IOutboundTopologyRegistry`. `PublishMessageAsync` and `PublishRawAsync` resolve the requested topology and then use only its outbound-target lookup surface. If the topology exists but has no matching outbound target, the existing outbound-target-not-found behavior should remain. `ForTopology(TopologyName)` still means "publish through this topology"; it does not imply that inbound endpoints are application-dispatched.

Default service registration should avoid ambiguity. `IMessagePublisher` can use `TopologyName.Default` as the default publish topology. A direct `ITopology` default service may be registered only if it clearly resolves `TopologyName.Default`; callers should not need default direction-specific topology services. Consuming-only topology instances should be reachable through `ITopologyRegistry` for observability, tests, validation, and management APIs, but their active behavior is started through topology runtime services.

Core should introduce a small runtime lifecycle seam for topology instances that have active background work:

```csharp
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface ITopologyRuntime
{
    TopologyName TopologyName { get; }

    Task StartAsync(CancellationToken cancellationToken = default);

    Task StopAsync(CancellationToken cancellationToken = default);
}
```

`ITopologyRuntime` should not be implemented by the Core `Topology` model itself. The topology model describes compiled declarations and dispatch definitions; the runtime describes active transport behavior such as RabbitMQ consumers, NATS subscriptions, SQS polling loops, and Azure Service Bus processors. Publish-only topologies do not need to register a runtime unless a future transport has publish-side background work.

A shared Core hosted service should depend on `IEnumerable<ITopologyRuntime>` and start each runtime after topology provisioning has completed. Shutdown should stop runtimes in a deterministic order and let each transport perform its own graceful drain. This replaces direction-specific consumer hosted-service discovery as the Core lifecycle seam, while still allowing transport runtimes to keep transport-specific state and behavior.

Core should not define `IConnectionProvider`, `IRecoverableConnection`, or similar abstractions in this slice. Connection/client semantics differ too much across RabbitMQ, NATS, Azure Service Bus, and SQS. The Core topology is the declaration and dispatch-definition aggregate; each transport owns its connection provider, recovery behavior, lifecycle hooks, and disposal details.

RabbitMQ should move from `AddRabbitMqOutboundTopology` and `AddRabbitMqInboundTopology` toward a unified `AddRabbitMqTopology(TopologyName, Action<RabbitMqTopologyBuilder>)` API. The builder should contain the shared resource surface (`UseConnectionFactory`, `Exchange`, `Queue`, `QueueBinding`, `ExchangeBinding`, `MapMessageContracts`) plus outbound-specific publishing configuration and inbound-specific consumer configuration. Existing direction-specific methods can remain temporarily as compatibility wrappers if that keeps the migration smaller, but they should compile to the same unified RabbitMQ topology model.

One RabbitMQ topology instance should own exactly one `RabbitMqConnectionProvider`. If a service wants RabbitMQ's recommended separation between publisher and consumer connections, it registers two topology instances with distinct names, for example `default` for publishing and `rabbitmq-consumers` for consuming. The consumer topology is still in the registry, provisioned, and hosted, but ordinary publishing call sites do not have to resolve it.

The compiled RabbitMQ topology should hold one Core `Topology` projection plus RabbitMQ-specific runtime state: exchanges, queues, bindings, addresses, outbound channel groups, inbound channel groups, outbound targets, inbound endpoints, pipeline, shutdown timeout, connection provider, and channel source. Direction-specific runtime pieces can remain as helper classes where useful, but there should not be two transport topology objects solely to mirror Core's old inbound/outbound split.

Provisioning should enumerate unified topology provisioners. RabbitMQ provisioners should provision the resources for each registered RabbitMQ topology. RabbitMQ should register an `ITopologyRuntime` only for topology instances that contain inbound endpoints. That runtime replaces or wraps the current inbound consumer hosted service: it opens consumer channels, starts `BasicConsume`, drains in-flight handlers on stop, and disposes runtime resources. Stop behavior, drain behavior, and acknowledgement behavior from the message-consumer implementation should be preserved.

Validation should be updated to the unified namespace. Duplicate topology names are rejected across all topology registrations. Within a topology, duplicate outbound target names, duplicate inbound endpoint names, duplicate RabbitMQ resource names, invalid channel-group references, missing serializers, missing handlers, and invalid message-contract mappings should still produce deterministic validation errors. A topology with no outbound targets is valid if it has inbound endpoints; a topology with no inbound endpoints is valid if it has outbound targets; an entirely empty topology should be rejected unless there is a clear transport-specific reason to allow it.

Tests should be migrated sociably through public registration APIs where possible. Important cases are: a default publish topology plus a named consuming topology, one topology containing both publishing and consuming definitions for transports that support it, duplicate names across publish-only and consume-only registrations, default publisher behavior when the default topology has outbound targets, failure when publishing through a consuming-only topology, `ITopologyRuntime` registration only for active topologies, runtime startup after provisioning, RabbitMQ one-connection-per-topology behavior, and RabbitMQ two-topology publish/consume isolation.
