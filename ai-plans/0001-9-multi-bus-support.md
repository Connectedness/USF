# Multi-Topology Outbound Publishing (Multi-Bus Support)

## Rationale

`AddRabbitMqOutboundTopology` registers everything as plain process-wide singletons — `IMessagePublisher`, `IOutboundTopology`, the compiled `RabbitMqOutboundTopology` (and with it one connection), the provisioner, and the hosted service. Calling it twice half-clobbers: the additive registrations (`IOutboundTopologyProvisioner`, `IHostedService`) both survive and provision, but the last-wins singletons (`IMessagePublisher`, `IOutboundTopology`) shadow the first registration, so you provision a topology you can never publish to. The provisioning side already takes `IEnumerable<IOutboundTopologyProvisioner>`; the registration/resolution side assumes exactly one topology. This slice makes multiple outbound topologies a first-class, named concept.

The selectable unit is the **outbound topology**, not a separate "bus": each topology owns its own connection (RabbitMQ's [production checklist](https://www.rabbitmq.com/docs/production-checklist#apps-connection-management) recommends separate publisher/consumer connections so broker backpressure on publishing cannot stall consumers), publishing only ever concerns the outbound side, and consumers are dispatched by the framework rather than selected. Inbound topologies are a separate plan and introduce no shared selection type, because there is nothing to select on the consume side.

The motivating scenario is a parallel-run migration (e.g. RabbitMQ → NATS over 6–12 months) where a service dual-publishes to two brokers and the **same CLR message type carries different discriminators/data schemas per broker** as the new system evolves. Outbound topology selection is therefore inherently a runtime decision, which shapes the publisher design (a single routing publisher rather than per-topology injected publishers) and the contract model (a canonical contract plus per-topology dialects).

This slice also folds the two currently-separate registration entry points (`AddCloudEvents` + `AddRabbitMqOutboundTopology`) into one coherent fluent root, and removes a hidden coupling that blocks per-topology contracts: the `CloudEventMessageSerializer` silently reads the shared contract registry for the `ce-dataschema` attribute.

The library is pre-1.0, so the breaking API changes below are acceptable.

## Acceptance Criteria

- [x] A coherent fluent root `services.AddUsf()` returns a `UsfBuilder` that registers the shared, once-only services (message-contract registry, `CloudEventMessageSerializer`, the single `OutboundTopologyHostedService`, the outbound-topology registry) and exposes `UseCloudEvents(...)` and `MapMessageContracts(...)`. Transport registration (`AddRabbitMqOutboundTopology`) is an extension method on `UsfBuilder` and returns it for chaining.
- [x] `AddRabbitMqOutboundTopology` gains an overload taking a `TopologyName`. The existing nameless overload registers the topology under `TopologyName.Default`. Registering two topologies under the same name throws `InvalidOperationException` listing the conflict; it never silently clobbers.
- [x] `TopologyName` is a `readonly record struct` in `Usf.Core` that validates a non-empty value, with a `Default` member. It is the only selection type; inbound naming is deferred to the inbound plan. No source generator is introduced.
- [x] Each outbound topology is registered as a keyed singleton (keyed by name) for `RabbitMqOutboundTopology`, `IOutboundTopology`, `OutboundTopology`, and `IOutboundTargetRegistry`. Each topology owns its own connection, channel groups, compiled targets, and effective contract registry, and is disposed independently by the container.
- [x] The RabbitMQ provisioner stays a non-keyed additive registration bound to its topology via a factory closure, so the single `OutboundTopologyHostedService` (registered exactly once regardless of topology count) enumerates and provisions all topologies. Provisioning never runs twice for one topology.
- [x] The deferred `() => topology!` closures in `RabbitMqOutboundTopologyCompiler` are removed: channel groups depend on a channel-source seam over the already-constructed `RabbitMqConnectionProvider`, not on the not-yet-built topology. No `null!`-bang capture of the topology under construction remains.
- [x] `RabbitMqOutboundTopologyCompiler` no longer depends on `IServiceProvider`; it takes the configuration, the canonical `IMessageContractRegistry`, an `ILoggerFactory`, and a serializer resolver (`Func<Type, IMessageSerializer?>`) as explicit dependencies, with the `RabbitMqConnectionProvider` constructed in the registration closure and passed in. It is unit-testable without building a `ServiceProvider`.
- [x] `IMessagePublisher` is a single routing singleton. `IMessagePublisher.ForTopology(TopologyName)` returns a zero-allocation `readonly struct TopologyPublisher` mirroring the publish surface; the default `IMessagePublisher` targets `TopologyName.Default`, so every existing single-topology call site compiles and behaves unchanged.
- [x] An explicit `OutboundTarget` is authoritative for topology selection (it is already topology-bound); passing both an explicit target and a non-default topology name that disagree throws. The topology-name path is only used for default-target-by-`T` resolution. Publishing to an unregistered topology, or to the default when none exists, throws a fail-fast error listing the registered outbound topologies — never a raw `KeyNotFoundException`.
- [x] `IOutboundTopologyRegistry` resolves a registered outbound topology (and its targets) by `TopologyName`; it is the supported opt-in for code that wants to bind to a named topology in its constructor instead of selecting at call time.
- [x] Message contracts support a **canonical** declaration (`MapMessageContracts` on the root, transport-agnostic, the source of truth for discriminators, data schemas, and inbound aliases) and an optional **per-topology dialect** (`MapMessageContracts` on the topology builder) that overrides the outbound discriminator and/or data schema for specific message types on that topology only. The same CLR type can publish under different discriminators on different topologies.
- [x] The compiled `RabbitMqOutboundTopology` owns a singleton effective `IMessageContractRegistry` for that topology (canonical with its dialect overlaid; dialect wins). Topologies with no dialect reuse the shared canonical registry. Every published type must resolve a discriminator from either the canonical contract or the topology dialect, validated at compile time against the effective registry (aggregated with the existing topology validation). A dialect entry for a message type that no target on the topology publishes fails validation (catching typos); a dialect-only type with no canonical mapping is permitted and resolves from the dialect.
- [x] `CloudEventMessageSerializer` no longer depends on `IMessageContractRegistry`. `IMessageSerializer.SerializeAsync` receives the resolved `dataSchema` alongside the already-passed `type`, so the serializer is a shared stateless singleton and contract resolution happens at the contract boundary.
- [x] `OutboundTarget<T>` holds a reference to its topology's effective contract registry and resolves both discriminator and data schema by `message.GetType()` (the runtime type), so publishing a subtype of `T` through a target is supported. `MessagePublisher` no longer depends on a contract registry.
- [x] Automated tests are written, including: registering two topologies that publish the same type under different discriminators and asserting isolation (two connections, correct per-topology `ce-type`/`ce-dataschema` on the wire); the same-name and unregistered-topology guards; subtype publishing; and that, once the existing tests' registration is migrated to the `AddUsf` root, their publish paths and on-the-wire output are unchanged through the default topology.
- [x] A BenchmarkDotNet memory benchmark confirms the zero-allocation property: `publisher.ForTopology(name).PublishMessageAsync(...)` allocates no more on the heap than the default `publisher.PublishMessageAsync(...)`, i.e. the `TopologyPublisher` struct adds no allocations.

## Technical Details

**The coherent root (`UsfBuilder`).** `AddUsf()` (in `Usf.Core`) returns a `UsfBuilder` wrapping `IServiceCollection` (exposing `.Services`). `UseCloudEvents(Action<CloudEventsOptions>)` and `MapMessageContracts(Action<MessageContractRegistryBuilder>)` replace the two arguments of today's `AddCloudEvents`, and the builder registers the once-only shared services that the current `AddRabbitMqOutboundTopology` registers per call (the hosted service, the topology registry) plus the canonical contract registry, `IPayloadCodec`, and `CloudEventMessageSerializer`. This deletes the marker-guard that would otherwise be needed to register `OutboundTopologyHostedService` exactly once — the root owns it structurally. `AddUsf` registers its shared services with `TryAdd`, so it is safe to call once and compose with other registrations; `UseCloudEvents` carries over the existing `CloudEventsOptions` source validation and `ValidateOnStart`, and `MapMessageContracts` is additive (multiple calls accumulate into the canonical builder). `AddRabbitMqOutboundTopology` is an extension on `UsfBuilder` only — there is deliberately no standalone `IServiceCollection` overload, so a topology cannot be half-configured without the shared root. `AddCloudEvents` is removed (pre-1.0); its two arguments are subsumed by `UseCloudEvents`/`MapMessageContracts`. Typical shape:

```csharp
services
    .AddUsf()
    .UseCloudEvents(o => o.Source = "/acme")
    .MapMessageContracts(c => c
        .Map<OrderCreated>("com.acme.orders.created.v2")
        .WithDataSchema("/schemas/v2/order-created")
     )
    .AddRabbitMqOutboundTopology(
        "rabbitmq-legacy",
        t =>
        {
            t.MapMessageContracts(c => c
                .MapOutbound<OrderCreated>("orders.created.v1") // dialect
                .WithDataSchema("/schemas/v1/order-created")
            );
            t.UseConnectionFactory(/* ... */);
            t.Publish<OrderCreated>(route => route
                .ToFanoutAddress(/* ... */)
                .WithSerializer<CloudEventMessageSerializer>()
            );
        }
     );
```

**Keyed per-topology registration + compiler decoupling.** The compiler stops being a service locator: `RabbitMqOutboundTopologyCompiler` no longer receives `IServiceProvider`. It takes exactly what compilation needs — the `RabbitMqOutboundTopologyConfiguration`, the canonical `IMessageContractRegistry`, an `ILoggerFactory`, and a serializer resolver `Func<Type, IMessageSerializer?>` (which also answers the existing "is this serializer registered?" validation, since a `null` return is the not-registered case). The `IServiceProvider` is genuinely needed only by the user's `UseConnectionFactory(Func<IServiceProvider, ConnectionFactory>)` delegate, which runs lazily on first connect — so the registration closure (which holds the SP) constructs the `RabbitMqConnectionProvider` and passes it into the compiler, which never references the SP. The compiler becomes container-agnostic and unit-testable without building a `ServiceProvider`; making it an instance type with these ctor-injected dependencies reads better than a long static parameter list, and the internal helper methods stay static. Per-topology services are keyed by `TopologyName`:

```csharp
services.AddKeyedSingleton<RabbitMqOutboundTopology>(name, (sp, _) =>
{
    var connectionProvider = new RabbitMqConnectionProvider(/* lazy factory capturing sp */, logger);
    var compiler = new RabbitMqOutboundTopologyCompiler(
        canonical: sp.GetRequiredService<IMessageContractRegistry>(),
        loggerFactory: sp.GetRequiredService<ILoggerFactory>(),
        resolveSerializer: t => (IMessageSerializer?)sp.GetService(t));
    return compiler.Compile(configuration, connectionProvider);
});
services.AddKeyedSingleton<IOutboundTopology>(name,
    (sp, key) => sp.GetRequiredKeyedService<RabbitMqOutboundTopology>(key).OutboundTopology);
// OutboundTopology, IOutboundTargetRegistry likewise keyed
```

The provisioner stays **non-keyed and additive** (keyed registrations do not appear in a plain `IEnumerable<T>`, which the hosted service relies on), bound to its topology via the keyed lookup:

```csharp
services.AddSingleton<IOutboundTopologyProvisioner>(sp =>
    new RabbitMqOutboundTopologyProvisioner(sp.GetRequiredKeyedService<RabbitMqOutboundTopology>(name)));
```

The same-name guard inspects existing `ServiceDescriptor`s for a keyed `RabbitMqOutboundTopology` under `name` before registering. Keyed DI is available (the solution pins `Microsoft.Extensions.DependencyInjection` 10.0).

**Removing the `() => topology!` deferral.** Today `Compile` captures `() => topology!` into each `RabbitMqChannelGroup` because a channel group's channel-creation delegate routes through `topology.CreateChannelAsync`, yet the topology is constructed *after* the channel groups (its ctor takes them). The cycle is artificial: the only things behind `topology.CreateChannelAsync` are the `RabbitMqConnectionProvider` (already constructed before the channel groups) and the once-only channel-budget validation. Introduce a small `RabbitMqChannelSource` over the connection provider that owns `CreateChannelAsync(CreateChannelOptions?, CancellationToken)` (acquire connection → validate budget once → create channel). Construction order becomes: connection provider → channel source → channel groups capturing `channelSource.CreateChannelAsync` (no topology reference) → process targets / compute worst-case budget → topology, whose public `CreateChannelAsync`/`GetConnectionAsync` forward to the channel source. The topology still owns disposal (channel groups, then connection provider). The only residual late value is the worst-case channel count — it depends on implicit per-target channel groups discovered during target processing, so it is genuinely computed late; hand it to the channel source as a set-once scalar, or precompute it from the target/channel-group *definitions* to remove even that. Either way no `null!` capture of an object under construction survives, and the budget-validation seam (`ValidateChannelBudgetOnceAsync`) moves from `RabbitMqOutboundTopology` onto the channel source.

**`TopologyName`.** `public readonly record struct TopologyName(string Value)` with non-empty validation (a factory or guarded ctor) and a `static TopologyName Default`. Ordinal string equality matches the codebase convention and is correct for keyed-DI key comparison. Apps declare their own `static readonly` names for IntelliSense; correctness comes from startup/compile validation listing registered names, not codegen.

**Canonical contract + per-topology dialect.** The topology builder gains `MapMessageContracts(Action<MessageContractRegistryBuilder>)`, which builds the dialect into its *own* small `IMessageContractRegistry` with the existing builder — canonical and dialect cannot share one builder, because `MessageContractRegistryBuilder.Build()` rejects two canonical discriminators for one type. The **effective registry** is therefore a new overlay `IMessageContractRegistry` in `Usf.Core` that consults the dialect first and falls back to canonical for `GetDiscriminator`/`GetDataSchema` (inbound resolution stays on canonical, since an outbound dialect adds no inbound mappings). A topology with no dialect uses the shared canonical instance directly (fast path — no overlay, no allocation). The effective registry is exposed from `RabbitMqOutboundTopology` as the per-topology source of truth and is the registry handed to the existing `MessageContractOutboundTopologyValidator`, so "every published type resolves a discriminator" is checked against canonical + dialect for free. A dialect may introduce a type with no canonical mapping (a dialect-only contract); inbound-side dialects are deferred to the inbound plan.

**Serializer decoupled from the registry.** Today `MessagePublisher` resolves the discriminator and threads it in as `type`, while `CloudEventMessageSerializer` independently reads `ce-dataschema` from its own injected registry — two registry consumers for two attributes. The fix completes the pattern: drop `IMessageContractRegistry` from `CloudEventMessageSerializer`, add a `string? dataSchema` parameter to `IMessageSerializer.SerializeAsync`, and have the caller pass both `type` and `dataSchema`. The serializer becomes a stateless shared singleton resolved from the SP exactly as it is today (no per-topology serializer instances, no `ActivatorUtilities` gymnastics for arbitrary serializer types). Its internal `type ?? ResolveType(...)` and `GetDataSchema(...)` fallbacks are removed.

**Targets resolve contract attributes (subtype support).** The compiler hands each `OutboundTarget<T>` a reference to its topology's effective `IMessageContractRegistry`. `OutboundTarget<T>.PublishCoreAsync` resolves the discriminator and data schema by `message.GetType()` and passes them to `SerializeAsync`. This keeps resolution by runtime type (so subtypes of `T` get their own discriminator) and removes the contract registry from `MessagePublisher` entirely. The publisher still needs the discriminator for the *up-front* diagnostics `messageType` tag, so the target exposes resolution by runtime type (e.g. `ResolveDiscriminator(message.GetType())`) that the publisher calls before starting the producer activity — no registry dependency returns to the publisher. Consequence to decide: with per-topology dialects the same CLR type can emit under different `messageType` tag values across topologies; if a stable tag is preferred, tag with the CLR type name and carry the discriminator as a separate tag (a small, optional telemetry change).

**Routing publisher + `ForTopology` + `TopologyPublisher`.** `MessagePublisher` becomes a single routing singleton depending on `IOutboundTopologyRegistry`. Per call it dispatches through an explicit `OutboundTarget` when given, otherwise resolves the default target for `T` from the selected topology. `ForTopology` returns a `readonly struct TopologyPublisher` capturing the singleton router plus the `TopologyName`, forwarding each publish call with the captured name:

```csharp
public readonly struct TopologyPublisher        // mirrors the publish surface; not IMessagePublisher
{
    private readonly MessagePublisher _router;
    private readonly TopologyName _topology;
    public Task PublishMessageAsync<T>(T message, OutboundTarget? target = null,
        CancellationToken ct = default) where T : ICloudEvent
        => _router.PublishMessageAsync(message, target, _topology, ct);
    // remaining overloads forward identically
}
```

To guarantee the zero-allocation property the struct does **not** implement `IMessagePublisher` (which would box on widening); it mirrors the three publish methods, and `ForTopology` returns the concrete struct so call sites keep it via `var`. `ForTopology` lives on the concrete router (and/or `IMessagePublisher`), not on the minimal publish surface the struct mirrors, to avoid a recursive `ForTopology` on the struct. No keyed `IMessagePublisher` and no default-publisher alias are needed — one router covers all topologies.

**`IOutboundTopologyRegistry`.** A once-only singleton over the keyed lookups: `IOutboundTopology Get(TopologyName)` / `bool TryGet(...)`, throwing a fail-fast error that lists registered names on miss. The router and any opt-in constructor-bound consumers go through it.

**Unchanged.** `RabbitMqConnectionProvider`, `RabbitMqOutboundTopologyProvisioner`, `OutboundTopologyHostedService`, `OutboundTopology`, the existing `*OutboundTopology*` type names, and the `IMessageContractRegistry`/`MessageContractRegistryBuilder` contracts stay as they are — the bundle was already cohesive per instance; only the wiring assumed singularity, and the serializer shed one dependency.

**Out of scope / deferred.** Inbound topologies and any inbound naming type; an `AddRabbitMqBus`-style umbrella (unnecessary while topologies own their own connections); a distinct `TopologyName` per transport; and any source generator for topology names.
