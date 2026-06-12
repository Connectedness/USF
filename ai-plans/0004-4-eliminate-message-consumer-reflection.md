## Rationale

Code review of the message-consumer slices ([0004-0](0004-0-message-consumers.md) through [0004-3](0004-3-rabbitmq-dedicated-inbound-outbound-topologies.md)) surfaced two related findings in the inbound dispatch path:

1. **Per-delivery reflection in the hot path.** `MessageHandlerInvoker.InvokeAsync` runs `MakeGenericType` + `MakeGenericMethod` + `MethodInfo.Invoke` for every delivery, even though the message type is fixed per endpoint and known when the topology is configured. The 0004-0 plan claims "no per-message reflective construction"; this slice makes that claim true.
2. **`Handle<TMessage, THandler>()`'s `THandler` is dead at runtime.** The invoker resolves `IMessageHandler<TMessage>` from the scope and never consults `InboundEndpoint.HandlerType`. Users must register the interface mapping themselves *and* repeat the handler type in the builder, and two endpoints for the same message type with different handlers silently both get whatever DI returns for the interface.

Both findings share one fix, and it requires no new machinery: `Handle<TMessage, THandler>()` is the place where both types are statically known *as generic type parameters*. A strongly typed dispatch delegate captured at that call site — resolve the concrete `THandler` from the scope, cast the message, call `HandleAsync` — gives reflection-free per-delivery dispatch with exactly the code a source generator would emit, while staying NativeAOT/trimming-safe because the generic instantiation is visible to the compiler at the call site. This mirrors the existing pattern on the outbound side (`CreateTargetCore<TMessage>` capturing typed routing-key factories). A Roslyn source generator was considered and rejected: it would have to recognize fluent-builder call sites syntactically (fragile against helpers, variables, and loops) and adds a permanent build-infrastructure tax, all to reconstruct type information the generic call site already provides. It becomes the right tool only if convention-based handler discovery (assembly scanning) is added later.

Resolving the *concrete* handler type also makes `THandler` honest: each endpoint dispatches to its declared handler even when several endpoints consume the same message type, and the registration extensions can auto-register the handler so users stop registering `IMessageHandler<TMessage>` mappings by hand — matching what users of MassTransit/Wolverine expect.

Out of scope for this slice: a consume-path benchmark in `Usf.Benchmarks` (worth doing once the inbound pipeline stabilizes), and eliminating the once-at-startup `MakeGenericMethod` calls in `RabbitMqTopologyCompiler` (`CreateTargetCore`/`CreateEndpointCore`) — those run once per target/endpoint at topology compilation and are not on the per-delivery path.

## Acceptance Criteria

- [x] A static factory in `Usf.Core` — `MessageHandlerInvocation.Create<TMessage, THandler>()` where `THandler : class, IMessageHandler<TMessage>` — returns a `MessageDelegate` that resolves the concrete `THandler` from `context.Services`, casts `context.Message` to `TMessage` (it must not be null), and calls `HandleAsync(message, context, context.CancellationToken)`.
- [x] `InboundEndpoint` accepts the dispatch delegate as a constructor parameter and exposes `public Task InvokeHandlerAsync(IncomingMessageContext context)`, which guards against a null context and a not-yet-deserialized `Message` before invoking the delegate. `InboundEndpoint<TMessage>`, `RabbitMqInboundEndpoint`, and `RabbitMqInboundEndpoint<TMessage>` thread the parameter through their constructor chains.
- [x] `RabbitMqInboundEndpointBuilder.HandleNamed<TMessage, THandler>` creates the delegate via `MessageHandlerInvocation.Create<TMessage, THandler>()` and stores it on `RabbitMqInboundHandlerDefinition`; the topology compiler passes it through `CreateEndpointCore` into the endpoint.
- [x] `HandleNamed<TMessage, THandler>` throws an `ArgumentException` when `typeof(THandler)` is an interface or an abstract class — the `class` constraint admits both, and auto-registering such a type as its own implementation would otherwise surface as an opaque DI activation error at first delivery.
- [x] The inbound pipeline terminal in `RabbitMqTopologyCompiler.BuildPipeline` becomes `static context => context.Endpoint.InvokeHandlerAsync(context)`. No service is resolved to perform dispatch, and no `MakeGenericType`/`MakeGenericMethod`/`MethodInfo.Invoke` executes on the per-delivery path.
- [x] `MessageHandlerInvoker` is deleted, including its `TryAddSingleton` registration in `UsfServiceCollectionExtensions.AddUsf`.
- [x] `AddRabbitMqTopologyCore` registers each `RabbitMqInboundHandlerDefinition.HandlerType` as a scoped service for its own concrete type via `TryAddScoped`, so all three registration entry points (`AddRabbitMqTopology`, `AddRabbitMqOutboundTopology`, `AddRabbitMqInboundTopology`) auto-register handlers. An existing user registration for the same concrete type (any lifetime) wins.
- [x] `RabbitMqTopologyCompiler.ValidateServiceRegistrations` validates that the concrete `handler.HandlerType` is registered instead of `IMessageHandler<TMessage>`, with the error message updated accordingly.
- [x] A test proves the reviewer's failure scenario is fixed: two inbound endpoints consuming the same message type with two different handler types each dispatch to their own handler.
- [x] `RabbitMqDedicatedTopologiesIntegrationTests` no longer registers `IMessageHandler<RabbitMqPublishMessage>` manually — the `Handle<,>()` declaration alone suffices.
- [x] All other affected tests are updated: Core tests that construct `InboundEndpoint<TMessage>` directly pass a factory-created delegate; `AddRabbitMqConsumeTopologyTests` drops now-redundant interface registrations and its "handler not registered" validation test is reworked to defeat auto-registration (see Technical Details).
- [x] XML docs on `Handle<,>`/`HandleNamed<,>` state that the handler is auto-registered as a scoped service and resolved by its concrete type per delivery, and that a different lifetime can be chosen by registering the handler type in the service collection before calling `AddRabbitMq*Topology` — auto-registration uses `TryAdd` and yields to an existing registration.

## Technical Details

### Dispatch delegate factory (`Usf.Core`)

Replace `MessageHandlerInvoker.cs` with `MessageHandlerInvocation.cs`. The delegate shape is the existing `MessageDelegate` (`Task (IncomingMessageContext)`) — the handler invocation *is* the pipeline terminal, so reusing the pipeline's delegate type is deliberate:

```csharp
public static class MessageHandlerInvocation
{
    public static MessageDelegate Create<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        return static context => context.Services
           .GetRequiredService<THandler>()
           .HandleAsync((TMessage) context.Message!, context, context.CancellationToken);
    }
}
```

The `static` lambda has no captures; the JIT/AOT compiler closes the generic instantiation at the `Handle<,>()` call site, so dispatch costs one delegate invocation plus one cast per delivery. The factory lives in Core so any transport (and any test) creates dispatch delegates the same way.

The returned delegate deliberately carries no guards — `InboundEndpoint.InvokeHandlerAsync` owns them on the framework path. Because the factory is public Core API, its XML docs must state the precondition: the delegate requires `context.Message` to be the deserialized message (non-null) and `context.Services` to be the per-delivery scope.

### Endpoint changes

`InboundEndpoint` gains a `MessageDelegate handlerInvocation` constructor parameter (null-checked), stored in a private field, and a public method that owns the guards currently in `MessageHandlerInvoker.InvokeAsync`:

```csharp
public Task InvokeHandlerAsync(IncomingMessageContext context)
{
    if (context is null)
    {
        throw new ArgumentNullException(nameof(context));
    }

    if (context.Message is null)
    {
        throw new InvalidOperationException("The inbound message has not been deserialized.");
    }

    return _handlerInvocation(context);
}
```

The delegate is exposed only through `InvokeHandlerAsync` — no public property — so the guards cannot be bypassed. `InboundEndpoint<TMessage>`, `RabbitMqInboundEndpoint`, and `RabbitMqInboundEndpoint<TMessage>` add the parameter to their constructor chains. The existing constructor validation that `handlerType` implements `IMessageHandler<TMessage>` stays; the constructor cannot verify that the delegate was built for the same handler type, but both values originate from the same `HandleNamed<TMessage, THandler>` call in practice.

`HandlerType` remains a property: it drives auto-registration, compiler validation, and diagnostics.

### Builder and compiler threading

`RabbitMqInboundHandlerDefinition` gains a `MessageDelegate HandlerInvocation` positional parameter. `HandleNamed<TMessage, THandler>` populates it with `MessageHandlerInvocation.Create<TMessage, THandler>()` alongside the existing `typeof(THandler)`, after rejecting interface and abstract `THandler` types with an `ArgumentException` (the `class` constraint admits both; previously harmless because `THandler` was unused at runtime, but auto-registration would turn such a type into an uninstantiable self-mapping that only fails at first delivery). `RabbitMqTopologyCompiler.CreateEndpointCore<TMessage>` passes `handlerDefinition.HandlerInvocation` into the `RabbitMqInboundEndpoint<TMessage>` constructor.

`BuildPipeline`'s terminal changes from resolving `MessageHandlerInvoker` to:

```csharp
return pipeline.Build(static context => context.Endpoint.InvokeHandlerAsync(context));
```

Delete `MessageHandlerInvoker` and remove `services.TryAddSingleton<MessageHandlerInvoker>()` from `UsfServiceCollectionExtensions.AddUsf` (UsfServiceCollectionExtensions.cs:44).

### Handler auto-registration

In `RabbitMqTransportModule.AddRabbitMqTopologyCore`, before the keyed-singleton registrations:

```csharp
foreach (var handler in configuration.Handlers)
{
    services.TryAddScoped(handler.HandlerType);
}
```

Scoped matches the per-delivery scope created by `RabbitMqTopologyRuntime`. `TryAddScoped` keeps an existing user registration for the same concrete type authoritative (e.g., a user who wants a singleton handler registers it before `AddRabbitMq*Topology`). Users who previously registered only the interface mapping (`AddScoped<IMessageHandler<X>, XHandler>()`) are unaffected: the concrete registration is added independently and the interface mapping simply becomes unused.

### Validation

`ValidateServiceRegistrations` switches from `typeof(IMessageHandler<>).MakeGenericType(handler.MessageType)` to `handler.HandlerType`, with a message along the lines of `Inbound handler '{handler.HandlerType}' for message '...' is not registered.` Through the public registration extensions this check can no longer fail (auto-registration precedes compilation), but it stays as a safety net: the service collection is mutable after `AddRabbitMq*Topology`, and the compiler is constructible independently of the extensions.

### Tests

- **New (Core):** `MessageHandlerInvocation`/`InvokeHandlerAsync` unit tests — the delegate resolves the concrete handler type from the provided scope (not the interface), the cast message reaches `HandleAsync`, and `InvokeHandlerAsync` throws when `Message` is null.
- **New (RabbitMq):** the distinct-handlers test — a topology with two `Consume` endpoints for the same message type and different handler types; invoking each endpoint's `InvokeHandlerAsync` (or driving the compiled dispatch index) reaches the endpoint's own handler. This pins the bug the reviewer identified. Plus a builder test asserting that `Handle<TMessage, THandler>` with an interface or abstract `THandler` throws `ArgumentException`.
- **Updated:** `RabbitMqDedicatedTopologiesIntegrationTests` drops the `AddScoped<IMessageHandler<RabbitMqPublishMessage>, RecordingPublishMessageHandler>()` line — the round trip passing proves auto-registration end to end. `AddRabbitMqConsumeTopologyTests` drops its interface registrations; the test expecting `Inbound handler service ... is not registered` must now defeat auto-registration to trigger the error — remove the handler's service descriptor (`services.RemoveAll(typeof(ValidationMessageAHandler))`) after calling `AddRabbitMqTopology` and before building the provider, and update the expected message. The Core tests constructing `InboundEndpoint<TMessage>` directly (`IncomingMessageContextTests`, `FrameworkMessageAcknowledgementMiddlewareTests`, `TopologyTests`) pass `MessageHandlerInvocation.Create<TMessage, TheirHandler>()` for the new constructor parameter.
