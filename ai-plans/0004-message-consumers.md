# Message Consumers (Inbound Topology)

## Rationale

USF has an Outbound Topology but no Inbound Topology. This plan introduces message consumption as a first-class, multi-topology concept that mirrors the outbound spine (`Builder → Configuration → Compiler → Topology object → Provisioner → keyed-by-TopologyName registration`) while adding the one thing outbound does not have: a **runtime**. Outbound is passive — provision, then publish on demand. Inbound is active — provision, then run: open consumers, resolve an endpoint per delivery, create a DI scope, push the message through a pipes-and-filters pipeline, invoke the endpoint, and settle the acknowledgement, with safe defaults throughout.

The guiding model is that a topology is **this service's projection of the broker**: which messages it publishes (outbound) and which it consumes (inbound), with active-vs-passive declare as the knob for "do I own this resource or merely assert it exists." Two RabbitMQ topologies (inbound and outbound) exist for backpressure management — the [production checklist](https://www.rabbitmq.com/docs/production-checklist#apps-connection-management) recommends separate publisher and consumer connections so publish-side backpressure cannot stall consumers. Broker resources (exchanges, queues, bindings, with Skip/Passive/Active declare) are available on **both** topology builders; declaring a resource on both sides in a listen-to-yourself process is safe because broker declares are idempotent.

The processing model is deliberately ASP.NET-Core-shaped: an `IncomingMessageContext` (the `HttpContext` analog), a `MessageDelegate`/`IMessageMiddleware` pipeline compiled once at startup, a per-message DI scope, a terminal endpoint invoker resolving `IMessageHandler<T>`, and framework-owned safe defaults (ACK on success, NACK→DLX on failure). The context is **CloudEvents-agnostic**: it carries only broker-neutral delivery data, and CloudEvents is one inspector/deserializer pair among potentially many, so non-CloudEvents payloads (e.g. S3 notifications) are first-class.

The library is pre-1.0, so the breaking refactor of the outbound infrastructure types below is acceptable.

## Acceptance Criteria

### Shared infrastructure refactor (step 1)

- [ ] `OutboundTopologyRegistrationCatalog` is generalized into an abstract `TopologyRegistrationCatalog` (holding the names/dup-guard logic and a `static FormatNames`) with a `protected abstract string Direction` used in error text, plus two trivial sealed subclasses `OutboundTopologyRegistrationCatalog` and `InboundTopologyRegistrationCatalog`. Inbound and outbound names are independent namespaces (two distinct instances/DI types), so a service may have both an inbound and an outbound `default`.
- [ ] `OutboundTopologyRegistry`/`SingleOutboundTopologyRegistry` are reshaped onto an abstract generic base `TopologyRegistry<TTopology>` (and `SingleTopologyRegistry<TTopology>`) carrying the identical `Names`/`GetRequiredTopology`/`TryGetTopology` body over a keyed `TTopology` lookup. Thin direction-named interfaces `IOutboundTopologyRegistry` and `IInboundTopologyRegistry` are kept for discoverability and DI lookups.
- [ ] `OutboundTopology`'s name-indexed lookup is lifted into an abstract generic base `Topology<TEntry>` (name → entry dictionary, `Entries`, `GetRequired(string)`, `TryGet(string)`). The by-`Type` index stays an outbound-only field on `OutboundTopology`. The base requires **no** shared interface across entries (the name is the caller-supplied dictionary key).
- [ ] Provisioning is generalized: a shared `ITopologyProvisioner { ProvisionAsync }` and a renamed `TopologyProvisioningHostedService` enumerate and provision both directions' resources. The existing `IOutboundTopologyProvisioner`/`OutboundTopologyHostedService` are migrated onto these. The hosted service is registered before any inbound consumer host so declares precede consumption.
- [ ] `RabbitMqChannelSource`'s channel-budget validation (worst-case channel count ≤ negotiated `channel_max`) is reusable by the inbound side unchanged.
- [ ] Existing outbound tests pass against the refactored shared types; no outbound behavior changes.

### Core inbound abstractions

- [ ] `IInboundTopology`/`InboundTopology` (on `Topology<InboundEndpoint>`) hold the registered endpoints with lookup by **endpoint name** only and an internal runtime dispatch index keyed by **(queue, discriminator)**. There is deliberately **no** by-`Type` public lookup, because no call site selects an endpoint — the framework dispatches.
- [ ] `InboundEndpoint`/`InboundEndpoint<T>` are an independent hierarchy that shares no base or interface with `OutboundTarget`.
- [ ] `IInboundTopologyRegistry` and `InboundTopologyRegistrationCatalog` exist, with the same multi-topology keyed-by-`TopologyName` registration and same-name guard as outbound.
- [ ] `IMessageSerializer` gains a deserialize counterpart (envelope/body + target `Type` → object), and the CloudEvents serializer implements it; the inbound half of `MessageContractRegistry` (`TryResolveType`, `acceptsCanonicalInbound`, inbound aliases) is wired into dispatch.

### Runtime (transport-neutral, in `Usf.Core`)

- [ ] A CloudEvents-agnostic `IncomingMessageContext` carries the raw delivery (`TransportMessage`), the resolved `InboundEndpoint`, the per-message scope `IServiceProvider`, the typed-erased `object? Message`, the `IMessageAcknowledgement`, and the `CancellationToken`. Arbitrary per-message data is stored via strongly-typed keys (`MessageContextKey<T>` with `SetItem`/`TryGetItem`/`GetRequiredItem`/`RemoveItem` over a lazily-initialized `Dictionary<object, object?>`), modeled on the `ValidationState`/`ValidationContextKey<T>` pattern. CloudEvents data is set under well-known keys (`CloudEventsContextKeys.Envelope`), never as a baked-in property.
- [ ] A broker-neutral `TransportMessage` base class exposes the promoted standardized message properties as first-class typed members (`ContentType`, `ContentEncoding`, `MessageId`, `CorrelationId`, `ReplyTo`, `Timestamp`, `Priority`, `TimeToLive`, `Redelivered`, `DeliveryAttempt`, `Source`, `TransportName`), an owned `byte[] Body` (copied off the callback-scoped client buffer), and a lossless `IReadOnlyDictionary<string, object?> Headers` plus a `TryGetHeaderString` coercion helper. Application headers are **not** forced to `string` inbound.
- [ ] A `MessageDelegate` (`Func<IncomingMessageContext, Task>`) pipeline is built **once at startup** from `IMessageMiddleware` and `Use(...)` registrations. Order is: framework exception/ack middleware (outermost) → replaceable deserialization middleware (first real step) → user middleware → terminal endpoint invoker. The pipeline is configured **per topology**.
- [ ] Per delivery the runtime: creates an async DI scope, resolves the endpoint and message type via a per-queue inspector (below), builds the context over the scope, runs the compiled pipeline, and disposes the scope on completion (success or fault).
- [ ] The terminal endpoint invoker resolves `IMessageHandler<T>` from the scope and calls `HandleAsync(T message, IncomingMessageContext context, CancellationToken)`; the handler receives the typed message directly (no `IncomingMessageContext<T>`).
- [ ] Endpoint resolution is split from the handler-set: an `IInboundMessageInspector` (per queue, replaceable) reads routing data and produces the endpoint-selection key and message type; the handler-set (per endpoint) maps discriminator → handler. The CloudEvents inspector is the implied default so the common case needs no explicit inspector; raw consumers opt in (`UseInspector<T>()` per queue). Startup validation rejects handler/inspector wiring gaps; an unresolved key follows the unknown-message policy (NACK→DLX).
- [ ] `IMessageAcknowledgement` exposes `AckAsync`/`NackAsync(requeue)`. Safe defaults (auto-ack mode): ACK on success; **NACK requeue=false (→ DLX)** on handler exception, deserialization failure, and unknown discriminator; NACK requeue=true on shutdown-cancellation. A per-endpoint `AckMode` allows manual settlement; default is auto.

### RabbitMQ inbound transport

- [ ] `AddRabbitMqInboundTopology(...)` and `AddRabbitMqInboundTopology(TopologyName, ...)` register an inbound topology keyed by name, owning its **own** connection (separate from any outbound topology). The connection factory must have `AutomaticRecoveryEnabled` (rejected otherwise, as outbound); consumer recovery is relied upon to re-establish subscriptions after a reconnect.
- [ ] The inbound builder exposes the **shared** resource surface (`UseConnectionFactory`, `Exchange`, `Queue`, `QueueBinding`, `ExchangeBinding`, `MapMessageContracts`) via the same base used by the outbound builder, and the inbound-only surface `Consume(queue, endpoint => { … })` with `Handle<T, THandler>()`, `PrefetchCount`, `Concurrency`, and `ConfigureInboundPipeline` (per topology).
- [ ] Consumer channel groups mirror the outbound channel-group UX with consume-side semantics: default one **long-lived** channel per endpoint, overridable to co-locate endpoints on a shared channel or spread one endpoint across N channels. The group knobs are `PrefetchCount` (QoS) and `ConsumerDispatchConcurrency` (not publisher confirms). Channel-budget validation against `channel_max` is reused.
- [ ] Inbound resources are provisioned through the shared provisioning hosted service; a separate `InboundConsumerHostedService` starts consumers after provisioning and performs **graceful drain** on `StopAsync` (stop accepting deliveries, drain in-flight handlers up to a configurable shutdown timeout, then close channels and the connection; undrained deliveries NACK+requeue).
- [ ] Automated tests are written (unit + RabbitMQ integration), including: multi-topology inbound isolation, endpoint dispatch by discriminator, scope-per-message lifetime, the safe-default ack/nack matrix, a custom per-queue inspector for a non-CloudEvents payload, prefetch/concurrency behavior, and graceful shutdown drain.

## Technical Details

**Shared-type refactor first.** This is step 1 and the rest builds on it. The mechanical moves are small because the outbound types were already cut along the right seams. `TopologyRegistrationCatalog` keeps `_names`/`_namesSet`/`Add`/`Contains`/`Names`/`FormatNames`; only the "outbound" noun in the duplicate-name message becomes `Direction`. `TopologyRegistry<TTopology>` keeps the exact body of `OutboundTopologyRegistry`, resolving `GetRequiredKeyedService<TTopology>(name)`; `OutboundTopologyRegistry : TopologyRegistry<IOutboundTopology>, IOutboundTopologyRegistry`. `Topology<TEntry>` holds the `IReadOnlyDictionary<string, TEntry>` name index and the three name-keyed lookups; `OutboundTopology` adds its `_targetsByMessageType` and the `GetRequiredTarget(Type)`/`<T>()`/`TryGetTarget(Type)` members on top. `ITopologyProvisioner` is `IOutboundTopologyProvisioner` minus the direction prefix; `OutboundTopologyHostedService` becomes `TopologyProvisioningHostedService` over `IEnumerable<ITopologyProvisioner>`. Two DI registrations of `IEnumerable<ITopologyProvisioner>` mixing inbound and outbound provisioners is fine — order is irrelevant for idempotent declares — but the inbound consumer host must be registered after it so consumption starts post-provisioning.

**Independent inbound entries; no shared entry contract.** `OutboundTarget` and `InboundEndpoint` share nothing behaviorally (publish vs. consume), so they are separate hierarchies. The map reuse via `Topology<TEntry>` needs no entry interface because the name is supplied as the dictionary key by the compiler, exactly as `OutboundTopology`'s constructor already takes `targetsByName`. `InboundEndpoint` carries `Name`/`TransportName`/`TopologyName` and its handler-set; `InboundEndpoint<T>` adds the typed handler resolution. The internal (queue, discriminator) dispatch index lives in the RabbitMQ runtime, not in the neutral `InboundTopology` surface.

**Context and strongly-typed keys.** `IncomingMessageContext` is a class with the typed members above plus the `ValidationState`-style item store:

```csharp
public sealed record MessageContextKey<T>(string Name);

public sealed class IncomingMessageContext
{
    private Dictionary<object, object?>? _items;
    public void SetItem<T>(MessageContextKey<T> key, T value);
    public bool TryGetItem<T>(MessageContextKey<T> key, out T value);
    public T GetRequiredItem<T>(MessageContextKey<T> key);
    public bool RemoveItem<T>(MessageContextKey<T> key);
    // Transport, Endpoint, Services, Message, Acknowledgement, CancellationToken
}
```

`Message` stays `object?`; the handler's `T message` parameter is the typed view, so no per-type generic context (and no per-message reflective construction) is needed. A typed `IMessageMiddleware<T>`/`IncomingMessageContext<T>` is explicitly deferred.

**Why the context is CloudEvents-agnostic.** Forcing `CloudEventEnvelope` into the context would make raw consumption second-class. Instead the CloudEvents inspector parses the AMQP binding and stores the envelope under `CloudEventsContextKeys.Envelope`; the CloudEvents deserialization middleware reads it. A custom inspector (e.g. for S3 notifications) stores its own typed key and never touches CloudEvents. The neutral-middleware story is served not by the header bag but by the **promoted standardized properties** on `TransportMessage`.

**`TransportMessage` shape and the header decision.** The base promotes the cross-broker "basic properties" to typed members so neutral middleware reads `context.Transport.CorrelationId`/`.ContentType`/`.Timestamp` without dictionary lookups or downcasts. The application-header bag stays `IReadOnlyDictionary<string, object?>` — inbound must faithfully represent arbitrary wire values (`x-death` lists, `x-delivery-count` longs, third-party `byte[]`/`int` headers), and the runtime itself reads `x-delivery-count`/`x-death` to compute `DeliveryAttempt`. This is a principled asymmetry with the outbound `CloudEventEnvelope` (string-valued): outbound controls its own output (always CloudEvents strings), inbound cannot. `byte[] Body` is an owned copy because RabbitMQ.Client's delivery buffer is valid only for the callback, which the pipeline + scope outlive. The RabbitMQ subclass `RabbitMqTransportMessage` adds `DeliveryTag`, `Exchange`, `RoutingKey`, `ConsumerTag`, and `IReadOnlyBasicProperties` as the full-fidelity escape hatch; the raw `ulong` ack token stays off the neutral base (acking flows through `IMessageAcknowledgement`).

```csharp
public abstract class TransportMessage
{
    public required string TransportName { get; init; }
    public required string Source { get; init; }          // queue / subject
    public required byte[] Body { get; init; }            // owned copy
    public string? ContentType { get; init; }
    public string? ContentEncoding { get; init; }
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public string? ReplyTo { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public byte? Priority { get; init; }
    public TimeSpan? TimeToLive { get; init; }
    public bool Redelivered { get; init; }
    public uint DeliveryAttempt { get; init; }            // 1-based
    public required IReadOnlyDictionary<string, object?> Headers { get; init; }
    public bool TryGetHeaderString(string name, out string? value);
}
```

The exact promote-vs-subclass line (`UserId`/`AppId`/`DeliveryMode`) stays RabbitMQ-specific until a second transport proves them neutral.

**Pipeline, inspection, and deserialization placement.** Inspection (routing) happens **before** the pipeline because it selects the endpoint, message type, and content interpretation — and the scope — that the pipeline runs over; it is the replaceable per-queue seam (`IInboundMessageInspector`). Deserialization is the **first middleware**, not a pre-pipeline step, so a malformed body becomes a normal pipeline exception flowing through the same exception→NACK→DLX policy and is covered by the tracing span, while remaining swappable for maximum customization. The pipeline is compiled once into a `MessageDelegate` and is per topology (matching the per-topology dialect/connection model). The terminal middleware is the endpoint invoker.

**Endpoint resolution split.** An endpoint key is `(queue, discriminator)`. The inspector emits that key plus the resolved CLR type; the runtime looks up the handler-set registered for the endpoint and dispatches. Splitting inspector from handler-set lets one inspector route a queue to many endpoints and makes the CloudEvents inspector reusable; the only obligation is startup validation that every registered discriminator resolves to a handler and that the inspector output is reachable. The simple case stays terse because the CloudEvents inspector is implied — `Consume(queue).Handle<T, H>()` needs no inspector mention.

**Acknowledgement and safe defaults.** `IMessageAcknowledgement` is a context service backed by the RabbitMQ channel + delivery tag. Framework outer middleware settles automatically in auto-ack mode: success → `AckAsync`; handler exception, deserialization failure, unknown discriminator → `NackAsync(requeue: false)` (→ DLX, quarantining poison rather than hot-looping); shutdown-cancellation → `NackAsync(requeue: true)`. Manual `AckMode` lets advanced handlers settle themselves. DLX wiring itself is the user's broker setup (declared via the shared resource surface); requeue-on-failure is opt-in per endpoint.

**RabbitMQ runtime and channels.** Each inbound topology owns one connection via a `RabbitMqConnectionProvider` (reused unchanged) and requires `AutomaticRecoveryEnabled`. Consumers are long-lived `IAsyncBasicConsumer` registrations, default one channel per endpoint for backpressure isolation (a slow endpoint's prefetch window and dispatch slots cannot starve another) and fault isolation (a channel error tears down only its own consumers). Consumer channel groups are a parallel concept to outbound channel groups — same named-grouping ergonomics, but the per-group knobs are `PrefetchCount` (`BasicQosAsync`) and `ConsumerDispatchConcurrency` (v7 per-channel callback parallelism) rather than publisher confirms; they are not the lease-and-return pool type, since consuming does not lease per message. The shared `RabbitMqChannelSource` budget check (worst-case channel count ≤ `channel_max`) applies. Whether the runtime should own re-subscription (custom backoff / circuit-breaking) instead of delegating to the client's consumer recovery is deferred.

**Lifecycle.** Provisioning (shared hosted service) runs first; `InboundConsumerHostedService.StartAsync` then opens channels, applies QoS, and issues `BasicConsumeAsync` per endpoint. `StopAsync` cancels consumers, drains in-flight handlers up to a configurable timeout, NACK+requeues whatever did not finish, then disposes channels and the connection (provider last), matching the outbound disposal ordering.

**Out of scope / deferred.** In-broker retry/backoff policies and DLQ replay helpers; request/reply; saga integration; typed `IncomingMessageContext<T>`/`IMessageFilter<T>`; an ASP.NET-style feature collection (the strongly-typed item bag suffices for now); pooled/`IMemoryOwner` bodies; runtime-owned consumer recovery; and retrofitting the outbound `CloudEventEnvelope` toward the promoted-properties shape.
