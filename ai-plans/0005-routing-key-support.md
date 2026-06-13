# Routing Key Support

## Rationale

Allow callers to pass a domain-driven routing key when publishing a message, treating it as an optional parameter for an already selected outbound target. Routing keys must stay simple `string` values and must not become a transport-specific DSL or replace `OutboundTarget` selection.

## Acceptance Criteria

- [x] `IMessagePublisher.PublishMessageAsync` overloads accept an optional routing key in addition to the optional `OutboundTarget` without introducing ambiguous positional string calls for topology selection.
- [x] Topology-scoped publishing through `TopologyPublisher` supports the same optional routing key behavior.
- [x] `OutboundTarget<T>`'s typed publish contract (`PublishAsync` / `PublishTypedCloudEventAsync`) carries the optional routing key; the non-generic `OutboundTarget.PublishSerializedAsync` raw path remains unchanged.
- [x] `PublishRawAsync` remains unchanged because `SerializedMessage` already carries routing information in its headers.
- [x] Routing keys are represented as `string` values and remain optional; a `null`, empty, or whitespace-only routing key is treated the same as omitted and falls back to existing target routing behavior.
- [x] `Usf.Core` contains no broker-specific routing logic; the routing key is carried through as an opaque `string?` and interpreted only by transport implementations.
- [x] For RabbitMQ typed publishing, a caller routing key that is not null, empty, or whitespace overrides both constant target routing keys and message-derived routing-key factories; when the caller routing key is null, empty, or whitespace, existing target routing behavior is preserved.
- [x] Existing target-based publishing behavior continues to work when no routing key is provided.
- [x] Automated tests need to be written.

## Technical Details

Extend the core typed publishing flow by adding optional `string? routingKey = null` parameters to the public `IMessagePublisher.PublishMessageAsync` overloads and the `TopologyPublisher` forwarding methods. Thread the routing key through `MessagePublisher`'s public/default-topology overloads, its core publish path, `OutboundTarget<T>.PublishAsync`, `OutboundTarget<T>.PublishCoreAsync`, and `PublishTypedCloudEventAsync` so every transport receives caller routing information without changing target resolution semantics. For the publisher-level overloads that take an `OutboundTarget?` (`IMessagePublisher`/`TopologyPublisher`/`MessagePublisher`), place `routingKey` directly after the target and keep `CancellationToken` last. For the target-less `OutboundTarget<T>.PublishAsync` overloads (the `ICloudEvent` convenience overload, the `in CloudEventMetadata` overload, and the explicit `type`/`dataSchema` overload), place `routingKey` after the final message/metadata/type/dataSchema argument
and before `CancellationToken`; all public `PublishAsync` overloads gain the optional routing key for API consistency. Changing the abstract `OutboundTarget<T>.PublishTypedCloudEventAsync` signature affects every subclass, so update all implementers accordingly: the RabbitMQ outbound targets plus the `BenchmarkTarget` (in `Usf.Benchmarks`) and the `RecordingTarget`/`ThrowingTarget` test doubles must adopt the new signature. Keep topology-scoped `MessagePublisher` implementation overloads explicit about their API shape: do not introduce a positional `string` routing-key slot that can be confused with `TopologyName` via implicit conversion. Inserting `routingKey` before the trailing `CancellationToken` is a source-breaking change for callers that pass the token positionally; update existing call sites and tests accordingly (acceptable pre-1.0).

Do not change `PublishRawAsync`; it is the only `IMessagePublisher` method that accepts `SerializedMessage` directly, and serialized messages already carry routing information in their headers.

For RabbitMQ, thread the supplied typed-publish routing key into `RabbitMqOutboundTarget<TMessage>` dispatch. The effective AMQP routing key should be the caller-supplied key when it is not null, empty, or whitespace (check with `string.IsNullOrWhiteSpace`), otherwise the target's existing routing key behavior. This means a non-blank caller routing key overrides both constant direct/topic target keys and message-derived routing-key factories. When a non-blank caller routing key is supplied, do not evaluate the target's constant routing-key path or message-derived routing-key factory; evaluate existing target routing behavior whenever the caller routing key is null, empty, or whitespace. Keep route headers separate from routing keys.

Update unit tests around `MessagePublisher`, `TopologyPublisher`, and RabbitMQ outbound targets to cover routing key propagation, default behavior when omitted, explicit target behavior, unchanged raw publishing, and the RabbitMQ override/default distinction. Extend `TestRabbitMqChannel` so RabbitMQ unit tests can observe the `routingKey` argument passed to `BasicPublishAsync`.
