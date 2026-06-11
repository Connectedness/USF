## Rationale

The RabbitMQ production checklist recommends using a dedicated connection for publishing and a dedicated connection for consuming (https://www.rabbitmq.com/docs/production-checklist#apps-connection-management): when a publishing connection is throttled by broker flow control, a shared connection would also stall consumer acknowledgements — precisely when the broker needs consumers to drain queues. Since one `RabbitMqTopology` owns exactly one `RabbitMqConnectionProvider`, the split already *works* by registering two topologies, but nothing guides users toward it.

This slice reintroduces direction-specific registration — `AddRabbitMqOutboundTopology` and `AddRabbitMqInboundTopology` — as a transport-level convenience over the unified topology model from [0004-1](0004-1-core-topology-refactoring.md). Direction-specific builder *interfaces* constrain the configuration surface at compile time: the outbound builder cannot register consumers, the inbound builder cannot register publish targets. Both compile to the same `RabbitMqTopologyConfiguration` and `RabbitMqTopology`; `Usf.Core` is not changed. `AddRabbitMqTopology` stays for scenarios where a single shared connection is appropriate (low-traffic services, tests).

The current parameterless `AddRabbitMqOutboundTopology`/`AddRabbitMqInboundTopology` compatibility wrappers both default to `Topology.DefaultName` and therefore collide when used together; this slice fixes that by giving inbound topologies their own default name, `default-inbound`. The outbound topology keeps `Topology.DefaultName` because publish call sites resolve the default topology; inbound topologies are only started via `ITopologyRuntime` and their name is purely a catalog/diagnostics identity.

Out of scope for this slice: application identity and `ConnectionFactory.ClientProvidedName` defaulting (separate plan), removal of the Address concept, and cross-service broker-resource ownership conventions.

## Acceptance Criteria

- [ ] A `public interface IRabbitMqTopologyBuilder<TSelf>` exposes the direction-neutral configuration surface: `UseConnectionFactory` (both overloads), `Exchange`, `Queue`, `QueueBinding`, `ExchangeBinding`, and `MapMessageContracts`, each returning `TSelf` for fluent chaining.
- [ ] A `public interface IRabbitMqOutboundTopologyBuilder : IRabbitMqTopologyBuilder<IRabbitMqOutboundTopologyBuilder>` adds the publish-only surface: `Address`, `Publish<TMessage>`, `PublishNamed<TMessage>`, the outbound `ChannelGroup` overload, `WithDefaultPublisherConfirmMode`, and `WithDefaultPublisherConfirmTimeout`.
- [ ] A `public interface IRabbitMqInboundTopologyBuilder : IRabbitMqTopologyBuilder<IRabbitMqInboundTopologyBuilder>` adds the consume-only surface: `Consume`, the inbound `ChannelGroup` overload, `ConfigureInboundPipeline`, `UseDeserializationMiddleware<TMiddleware>`, and `WithShutdownTimeout`. It does not expose `Address`.
- [ ] `RabbitMqTopologyBuilder` implements both interfaces; its own public members are unchanged. `Build()` is not part of either interface.
- [ ] `AddRabbitMqOutboundTopology` accepts `Action<IRabbitMqOutboundTopologyBuilder>` and defaults the topology name to `Topology.DefaultName`; an overload accepts an explicit name.
- [ ] `AddRabbitMqInboundTopology` accepts `Action<IRabbitMqInboundTopologyBuilder>` and defaults the topology name to a new `public const string RabbitMqTopology.DefaultInboundName = "default-inbound"`; an overload accepts an explicit name.
- [ ] Calling `AddRabbitMqOutboundTopology(...)` and `AddRabbitMqInboundTopology(...)` without explicit names in the same application registers two topologies (`default` and `default-inbound`) — and therefore two connections — without a catalog collision.
- [ ] Both direction-specific registration methods share the registration pipeline of `AddRabbitMqTopology` (keyed `RabbitMqTopology`/`Topology` singletons, catalog entry, provisioner, conditional runtime); `AddRabbitMqTopology` itself is unchanged in behavior.
- [ ] XML docs on the three registration methods explain the dedicated-connection recommendation (with a link to the production checklist) and when to prefer the unified `AddRabbitMqTopology`. Existing XML docs that describe the unified model (the `RabbitMqTopologyBuilder` class doc, the `RabbitMqTopology` remark about registering separate topology instances for separate connections) are updated to reference the direction-specific registration methods.
- [ ] The existing `RabbitMqInboundIntegrationTests` are renamed and converted to register a dedicated outbound topology and a dedicated inbound topology via the new registration methods, exercising the publish-and-consume round trip across two connections. Other existing tests affected by the restructured registration methods are updated accordingly.

## Technical Details

### Builder interfaces

Three new files in `Usf.Transport.RabbitMq`: `IRabbitMqTopologyBuilder.cs`, `IRabbitMqOutboundTopologyBuilder.cs`, `IRabbitMqInboundTopologyBuilder.cs`. The base interface uses the self-referencing generic pattern so shared members chain in the derived interface type:

```csharp
public interface IRabbitMqTopologyBuilder<TSelf>
    where TSelf : IRabbitMqTopologyBuilder<TSelf>
{
    TSelf UseConnectionFactory(ConnectionFactory connectionFactory);
    TSelf UseConnectionFactory(Func<IServiceProvider, ConnectionFactory> createConnectionFactory);
    TSelf Exchange(string name, string type, Action<RabbitMqExchangeBuilder>? configure = null);
    // Queue, QueueBinding, ExchangeBinding, MapMessageContracts analogous
}
```

Member signatures mirror the existing `RabbitMqTopologyBuilder` methods exactly (parameter names, defaults, nested builder delegates such as `Action<RabbitMqInboundEndpointBuilder>` stay concrete). The interfaces live in the transport project and carry the `RabbitMq` prefix deliberately — their members reference `ConnectionFactory`, confirm modes, and prefetch counts, so they must not masquerade as Core abstractions.

The primary XML documentation for the shared members moves to the base interface; the concrete builder's members reference it via `<inheritdoc/>` where the texts coincide.

### `RabbitMqTopologyBuilder` implementation

C# does not allow covariant return types on interface implementations, so the existing public members (returning `RabbitMqTopologyBuilder`) cannot implicitly satisfy the interfaces. The builder keeps its public surface as-is and adds explicit interface implementations as one-line bridges. Because the class implements *two* instantiations of the generic base interface (`IRabbitMqTopologyBuilder<IRabbitMqInboundTopologyBuilder>` and `IRabbitMqTopologyBuilder<IRabbitMqOutboundTopologyBuilder>`), each shared member needs two explicit bridges; the direction-specific members need one bridge each for their declaring interface:

```csharp
IRabbitMqInboundTopologyBuilder IRabbitMqTopologyBuilder<IRabbitMqInboundTopologyBuilder>.Exchange(
    string name, string type, Action<RabbitMqExchangeBuilder>? configure) => Exchange(name, type, configure);
IRabbitMqOutboundTopologyBuilder IRabbitMqTopologyBuilder<IRabbitMqOutboundTopologyBuilder>.Exchange(
    string name, string type, Action<RabbitMqExchangeBuilder>? configure) => Exchange(name, type, configure);
```

Place the explicit implementations as the last members of the class so the primary surface stays readable.

This is the "public types hidden in plain sight" approach: the registration methods hand out the narrow interface, the concrete builder stays public, and a user who downcasts to `RabbitMqTopologyBuilder` is knowingly stepping outside the contract. No runtime enforcement is added — the constrained interfaces make the misuse impossible to write *accidentally*.

### Registration methods

In `RabbitMqTransportModule`, replace the current pass-through wrappers. Extract the body of `AddRabbitMqTopology(UsfBuilder, string, Action<RabbitMqTopologyBuilder>)` into a private `AddRabbitMqTopologyCore(UsfBuilder builder, string topologyName, RabbitMqTopologyConfiguration configuration)` that performs everything after the builder callback (catalog add, keyed singletons, provisioner, conditional runtime). All three public entry points then differ only in how they produce the configuration:

```csharp
public static UsfBuilder AddRabbitMqInboundTopology(
    this UsfBuilder builder,
    string topologyName,
    Action<IRabbitMqInboundTopologyBuilder> configure)
{
    // guards...
    var topologyBuilder = new RabbitMqTopologyBuilder();
    configure(topologyBuilder);
    return AddRabbitMqTopologyCore(builder, topologyName, topologyBuilder.Build());
}
```

The parameterless-name overloads delegate with `Topology.DefaultName` (outbound) and `RabbitMqTopology.DefaultInboundName` (inbound). `TopologyRegistrationCatalog` already rejects duplicate names across all entry points, so no additional collision handling is needed.

`RabbitMqTopology.DefaultInboundName` is a `public const string` on `RabbitMqTopology`, mirroring `Topology.DefaultName` — the constant is transport-level by design; Core must not learn about the inbound/outbound split.

No changes to compilation, provisioning, or runtime startup: a configuration produced through `IRabbitMqInboundTopologyBuilder` simply has no outbound targets (and vice versa), and the existing machinery — `HasInboundEndpoints`-gated runtime registration, the `TopologyRecoveryEnabled` validation for inbound topologies, the empty-topology warning — applies unchanged.

### Tests

The main test of this slice: rename `RabbitMqInboundIntegrationTests` and convert its single unified `AddRabbitMqTopology` registration into a dedicated `AddRabbitMqOutboundTopology` plus `AddRabbitMqInboundTopology` pair using the default names, so the end-to-end round trip runs over two topologies and two connections.

Beyond that, fix any existing tests that the restructured registration methods break — in particular tests that call the current `AddRabbitMqOutboundTopology`/`AddRabbitMqInboundTopology` pass-through wrappers with `Action<RabbitMqTopologyBuilder>` delegates, which must switch to the new interface delegates (and to the new inbound default name where they relied on `Topology.DefaultName`). The compile-time surface restriction needs no dedicated tests; it is enforced by the type system.
