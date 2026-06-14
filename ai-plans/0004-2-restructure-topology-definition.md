## Rationale

The previous slice ([0004-1](0004-1-core-topology-refactoring.md)) collapsed the Core topology model to a single `TopologyDefinition` aggregate, but kept it as a separate object that each transport topology *wraps*. `RabbitMqTopology` has-a `TopologyDefinition`. This composition seam buys nothing: every broker topology fully owns and re-exposes its definition, and callers pay for a `topology.Definition.GetRequiredTarget(...)` double-hop.

This refactor removes `TopologyDefinition` as a concept the user has to think about. There is simply a `Topology`, and each message broker USF addresses contributes a dedicated subtype (`RabbitMqTopology`). A RabbitMQ topology *is* a topology, so inheritance replaces composition. The result is fewer objects in memory and a smaller conceptual surface.

Breaking changes are welcome here, we have not published the library yet.

## Acceptance Criteria

- [x] `TopologyDefinition` is removed from the codebase; no compatibility alias remains.
- [x] `Usf.Core` exposes a `public abstract class Topology` that carries the topology name, outbound targets, and inbound endpoints, plus the existing resolve/try-get lookup surface for targets (by name and by message type) and endpoints (by name).
- [x] Each transport topology derives from `Topology`; `RabbitMqTopology : Topology`.
- [x] Topology lookups remain backed by immutable data structures (`FrozenDictionary<TKey, TValue>`, `ImmutableArray<T>`) with ordinal string comparison.
- [x] `Topology` exposes `IsEmpty` (no outbound targets and no inbound endpoints), `IsOutboundOnly` (only outbound targets), and `IsInboundOnly` (only inbound endpoints).
- [x] The frozen-dictionary/immutable-array assembly logic is moved out of the constructor into a static `PrepareTopologyDataStructures` method on a `public readonly record struct TopologyData`; the `Topology` constructor performs only field assignment from a `TopologyData` value.
- [x] `TopologyName` is removed entirely from the codebase and replaced with `string`; its null/whitespace validation is enforced both at registration time (`TopologyRegistrationCatalog.Add`) and in the `Topology` constructor, and its `Default` sentinel becomes a `Topology.DefaultName` constant.
- [x] `Topology` does not implement `IDisposable` or `IAsyncDisposable`; `RabbitMqTopology` continues to implement both.
- [x] `ITopologyRegistry`, `TopologyRegistry`, `SingleTopologyRegistry`, and `TopologyRegistrationCatalog` work with `Topology` and `string` topology names.
- [x] RabbitMQ registers a keyed `RabbitMqTopology` per topology name plus a keyed `Topology` that delegates to it, so both the concrete and base service types resolve the same instance.
- [x] Existing outbound publishing and inbound consumer behavior are preserved.
- [x] Automated tests are updated for the new `Topology` base type, the `IsEmpty`/`IsOutboundOnly`/`IsInboundOnly` properties, `string`-keyed registration, and duplicate topology-name rejection.

## Technical Details

### `Topology` base class

Introduce `public abstract class Topology` in `Usf.Core.Messaging`, replacing `TopologyDefinition`. It holds the three private `FrozenDictionary` fields (targets by message type, targets by name, endpoints by name) plus the public `Name` (`string`), `OutboundTargets` (`ImmutableArray<OutboundTarget>`), and `InboundEndpoints` (`ImmutableArray<InboundEndpoint>`) members. The resolve/try-get methods (`GetRequiredTarget(Type)`, `GetRequiredTarget<T>()`, `GetRequiredTarget(string)`, `GetRequiredTarget<T>(string)`, `TryGetTarget(...)`, `GetRequiredEndpoint(string)`, `TryGetEndpoint(string)`) carry over verbatim from `TopologyDefinition`.

The constructor is `protected` and takes the topology `string` name plus a single `TopologyData` value. It guards the `name` argument against null/whitespace (this is the relocated `TopologyName` validation) and otherwise does nothing but assign fields. This is consistent with the project's "no `internal`; lower-level APIs hidden in plain sight via placement" convention — the base stays on the public surface but is only meaningfully constructed by subclasses.

Expose the default topology name as a `public const string DefaultName = "default"` on `Topology`, replacing the removed `TopologyName.Default` sentinel.

Add three computed properties. They use `ImmutableArray<T>.IsDefaultOrEmpty` (rather than `IsEmpty`, which throws on a `default` array) so they remain safe regardless of how the arrays were initialized:

- `IsEmpty => OutboundTargets.IsDefaultOrEmpty && InboundEndpoints.IsDefaultOrEmpty`
- `IsOutboundOnly => !OutboundTargets.IsDefaultOrEmpty && InboundEndpoints.IsDefaultOrEmpty`
- `IsInboundOnly => OutboundTargets.IsDefaultOrEmpty && !InboundEndpoints.IsDefaultOrEmpty`

`RabbitMqTopologyCompiler.WarnWhenEmpty` should lean on `IsEmpty` rather than re-deriving the condition. It must preserve its current behavior: an empty topology is *warned about*, not rejected. This refactor does not turn the empty-topology warning into a validation failure.

The unused `RabbitMqTopology.HasInboundEndpoints` property (currently `Endpoints.Count > 0`, with no callers) is removed. `RabbitMqTopologyConfiguration.HasInboundEndpoints` is unaffected: it is computed from the configuration at registration time — before any topology is compiled — and stays as is.

### `TopologyData`

Introduce `public readonly record struct TopologyData` carrying the three `FrozenDictionary` lookups and the two `ImmutableArray` projections. Its static `PrepareTopologyDataStructures(...)` method takes the source `IDictionary<Type, OutboundTarget>`, `IDictionary<string, OutboundTarget>`, and `IDictionary<string, InboundEndpoint>` (the same inputs the current `TopologyDefinition` constructor accepts), performs the null checks, freezes the dictionaries with `StringComparer.Ordinal`, and builds the `OutboundTargets`/`InboundEndpoints` arrays. The arrays must be computed here, not in the `Topology` constructor, so the constructor stays pure assignment. The record struct is a transient bundle passed by value once at construction and immediately unpacked into fields — it must not be stored.

### Removing `TopologyName`

`TopologyName` is the only remaining anti-primitive-obsession wrapper. Remove it entirely and replace every usage with `string`. Two guarantees it currently provides must be relocated, not lost:

- **Validation.** The null/whitespace guard in `TopologyName`'s constructor is enforced in two places: `TopologyRegistrationCatalog.Add`, so an invalid name fails eagerly at registration time (matching today's behavior, where the `TopologyName` conversion threw at the call site), and a guard clause on the `Topology` constructor's `name` argument as defense-in-depth, so an invalid name cannot reach a constructed topology even if a future caller bypasses the catalog.
- **The `Default` sentinel.** `TopologyName.Default = "default"` becomes `Topology.DefaultName`. Replace all `TopologyName.Default` references, including the default-topology branch in `RabbitMqTransportModule`.

`TopologyRegistrationCatalog` switches its `List<TopologyName>`/`HashSet<TopologyName>` to `string` collections with `StringComparer.Ordinal` set explicitly. Keyed-DI keys switch from `TopologyName` to `string` (default keyed-service equality is ordinal, preserving current semantics).

### Registry and publisher seams

`ITopologyRegistry`, `TopologyRegistry`, and `SingleTopologyRegistry` change their `TopologyDefinition` references to `Topology` and their `TopologyName` references to `string`. `MessagePublisher`'s constructor parameter and `UsfServiceCollectionExtensions`' non-keyed convenience registration change from `TopologyDefinition` to `Topology`. The registry remains broker-agnostic and returns the `Topology` base type; provisioner and runtime registrations that need broker specifics continue to resolve the concrete keyed `RabbitMqTopology` and are unaffected.

### RabbitMQ wiring

`RabbitMqTopology` derives from `Topology`. `RabbitMqTopologyCompiler.Compile` constructs the `RabbitMqTopology` directly with a `TopologyData` value (built via `TopologyData.PrepareTopologyDataStructures`) plus the broker-specific runtime state, rather than constructing a separate `TopologyDefinition` and passing it in. The `Definition` property is removed; callers that previously read `topology.Definition.X` read `topology.X`.

In `RabbitMqTransportModule`, keep two keyed registrations, but have the base-type registration delegate to the concrete instance rather than projecting a separate `TopologyDefinition` object:

```csharp
services
   .AddKeyedSingleton<RabbitMqTopology>(topologyName, (sp, _) => compiler.Compile(...))
   .AddKeyedSingleton<Topology>(topologyName, (sp, k) => sp.GetRequiredKeyedService<RabbitMqTopology>(k));
```

This is one instance with two service-type views: broker-specific consumers (provisioner, runtime, the non-keyed default convenience registration) resolve the keyed `RabbitMqTopology` as today, while Core's `TopologyRegistry` resolves the keyed `Topology` — which .NET keyed DI matches by exact service type, so the base-type registration is required. Keeping both keyed views is more flexible than registering only `Topology` and casting at every broker-specific call site. The previous `AddKeyedSingleton<TopologyDefinition>` registration is removed because `TopologyDefinition` no longer exists.

### Documentation

XML-doc comments in `RabbitMqTopology` and `RabbitMqTopologyCompiler` that reference "one Core `TopologyDefinition`" must be updated to describe the unified `Topology` base.
