# Eliminate RabbitMQ Headers and Body Copying

## Rationale

Code review of the message-consumer slices found redundant per-delivery copying in the inbound path:

1. **The body is copied twice.** `RabbitMqTransportMessage` passes `body.ToArray()` to the base constructor (RabbitMqTransportMessage.cs:26), and `TransportMessage` defensively clones that already-private array again (TransportMessage.cs:32).
2. **The header dictionary is built three times.** Once as the constructor argument (`CopyHeaders(basicProperties)`), a second time inside the base constructor (`TransportMessage.CopyHeaders`), and a third time inside `GetDeliveryAttempt` (RabbitMqTransportMessage.cs:146), which copies the full dictionary just to perform two lookups.

The target state is **one body copy and one dictionary build per delivery by default, and zero body copies as an opt-in**. Zero-copy is possible because the entire inbound pipeline (`ProcessDeliveryAsync`) is awaited inside the RabbitMQ.Client delivery callback, and the client guarantees the pooled body buffer stays valid for the duration of that callback. It must remain opt-in because the buffer is reused afterwards: a handler that retains the message past its own invocation would silently read foreign bytes.

Design decisions already settled:

- `TransportMessage.Body` becomes `ReadOnlyMemory<byte>` (breaking change; we are pre-release). This closes the existing mutability hole (`byte[]` lets handlers mutate the shared body) and is what makes zero-copy representable at all.
- The base constructor stores both `body` and `headers` exactly as passed — no defensive copy of either. The caller owns the responsibility of passing values it does not mutate afterwards; call sites that downcast the dictionary or alias a buffer do so at their own risk. This is one uniform ownership contract, documented and not enforced. The copy decision for the body lives entirely in `RabbitMqTransportMessage` (the only party that knows whether its buffer is pooled), not in Core.

Consume-path benchmarks stay deferred as decided in [0004-4](0004-4-eliminate-message-consumer-reflection.md); the win here is the removal of obvious allocations, verifiable by inspection and tests.

## Acceptance Criteria

- [ ] `TransportMessage.Body` is of type `ReadOnlyMemory<byte>`. The constructor accepts `ReadOnlyMemory<byte> body` and stores it as-passed, with no defensive copy and no `copyBody` parameter in Core. No copy of the body exists on the inbound path beyond the single optional copy the transport may choose to make.
- [ ] `TransportMessage` stores the `headers` argument directly (null check only); `TransportMessage.CopyHeaders` is deleted. The constructor's XML docs state that the message takes ownership of both the body and the dictionary and that callers must not mutate either after construction.
- [ ] `RabbitMqTransportMessage` builds its header dictionary exactly once per delivery: `CopyHeaders(basicProperties)` remains the single build, and `GetDeliveryAttempt` reads `basicProperties.Headers` directly instead of copying.
- [ ] `RabbitMqTransportMessage` owns the body-copy decision via a `bool copyBody` constructor parameter, passing `copyBody ? body.ToArray() : body` to the base.
- [ ] `CloudEventEnvelope.Data` becomes `ReadOnlyMemory<byte>` with content-based equality preserved (equal content in distinct backing arrays compares equal; differing content compares unequal; equal content yields equal hash codes); `IPayloadCodec.Decode` and `Utf8JsonPayloadCodec.Decode` accept `ReadOnlyMemory<byte>` and deserialize from the span without an intermediate array.
- [ ] `RabbitMqInboundEndpointBuilder` exposes a zero-copy opt-in (`ZeroCopyBody()`), captured per `Handle`/`HandleNamed` call like the existing builder settings, threaded through `RabbitMqInboundHandlerDefinition` and `RabbitMqInboundEndpoint` into `RabbitMqTopologyRuntime`, which passes `copyBody` accordingly when constructing `RabbitMqTransportMessage`. The default copies.
- [ ] `RabbitMqTopologyCompiler` validation fails when handler definitions for the same queue disagree on the zero-copy setting, since the copy decision is made once per delivery before discriminator dispatch.
- [ ] XML docs on `ZeroCopyBody()`, the `copyBody` constructor parameter, and `TransportMessage.Body` state the lifetime contract: with zero-copy enabled, `Body` (and any value derived from it without copying, such as `CloudEventEnvelope.Data`) references transport-owned pooled memory that is only valid until the message handler completes; the message must not be retained, processing must not be offloaded past the handler's lifetime, and violations read reused buffer contents rather than throwing.
- [ ] Automated tests are written/updated, including: header dictionary built once and delivery attempt still computed correctly from `x-delivery-count`/`x-death`/`redelivered`; `copyBody: false` aliases the input memory while the default does not; topology validation rejects mixed zero-copy settings on one queue.

## Technical Details

### `TransportMessage` (Usf.Core)

The constructor signature changes from `byte[] body` to `ReadOnlyMemory<byte> body`, dropping the null check (a `default` memory is an empty body, which is already permitted today). Both `body` and `headers` are stored exactly as passed — Core performs no defensive copy of either and has no `copyBody` parameter. `TryGetHeaderString` is unaffected. The ownership contract (caller must not mutate body or headers after construction) lives in the constructor's XML docs.

### `RabbitMqTransportMessage`

The constructor gains a `bool copyBody` parameter and owns the body-copy decision — it is the only party that knows the `eventArgs.Body` buffer is rented from RabbitMQ.Client's pool — passing `copyBody ? body.ToArray() : body` to the base. `ReadOnlyMemory<byte>.ToArray()` always allocates a fresh array, so the default path's defensive copy is preserved. The private `CopyHeaders(basicProperties)` stays as the single dictionary build (it also normalizes the headers-absent case to an empty `ReadOnlyDictionary`). `GetDeliveryAttempt` changes signature to keep taking `IReadOnlyBasicProperties` but performs its `x-delivery-count` and `x-death` lookups via `basicProperties.Headers.TryGetValue` directly, guarded by `IsHeadersPresent()`/null; `TryGetUnsignedHeader` adapts to `IDictionary<string, object?>` or is inlined.

`RabbitMqTransportMessage` also retains `BasicProperties` (`IReadOnlyBasicProperties`). RabbitMQ.Client 7.x is expected to materialize property strings and the headers dictionary eagerly during frame parsing, making retention safe even under zero-copy — but the implementer must confirm this against the client source. If any property lazily references the frame buffer, it carries the same lifetime hazard as the body and the zero-copy docs (and possibly the copy logic) must cover it.

### CloudEvents path

`CloudEventEnvelope.Data` changes to `ReadOnlyMemory<byte>`. Generator.Equals' `[OrderedEquality]` does not apply to `ReadOnlyMemory<byte>`; use `[property: CustomEquality(...)]` with a small comparer that compares via `Span.SequenceEqual` (and hashes cheaply, e.g. over length or a prefix — equality correctness matters, hash quality does not for this type). `CloudEventsInboundMessageInspector` passes `transportMessage.Body` through unchanged. `IPayloadCodec.Decode(byte[] ...)` becomes `Decode(ReadOnlyMemory<byte> ...)`; `Utf8JsonPayloadCodec` calls `JsonSerializer.Deserialize(data.Span, typeInfo)` and drops the null check. The outbound side keeps producing `byte[]` (`EncodedPayload`, `SerializedMessage` are out of scope); the implicit `byte[]` → `ReadOnlyMemory<byte>` conversion covers envelope construction, and `IChannel.BasicPublishAsync` already takes `ReadOnlyMemory<byte>`.

This matters for the opt-in: with zero-copy enabled, the bytes flow pooled-buffer → `Body` → `Envelope.Data` → `Deserialize(span)` with no copy anywhere, and deserialization happens inside the delivery callback via `MessageDeserializationMiddleware`, i.e. inside the validity window.

### Zero-copy opt-in plumbing

`RabbitMqInboundEndpointBuilder` gains a `bool _copyBody = true` field and a `ZeroCopyBody()` method; the value is captured into a new `bool CopyBody` positional parameter on `RabbitMqInboundHandlerDefinition` and threaded by `RabbitMqTopologyCompiler.CreateEndpointCore` into a `CopyBody` property on `RabbitMqInboundEndpoint` (constructor parameter, like `QueueName`). `RabbitMqTopologyRuntime.ProcessDeliveryAsync` reads `subscribedEndpoint.CopyBody` when constructing the message.

Because the transport message is constructed before the inspector resolves the discriminator, the setting is effectively per queue. The compiler's existing validation pass gains a check that all inbound handler definitions sharing a queue have the same `CopyBody` value, failing topology compilation with a message naming the queue.

### Forward-looking constraint

Zero-copy establishes a standing invariant: **nothing may retain the transport message or its body beyond the delivery callback.** Today this holds by construction — the message is consumed synchronously within `ProcessDeliveryAsync`. Any future feature that retains the message or its body past the callback (e.g. an outbox, delayed retry, or dead-letter-with-body) must either force a copy or reject zero-copy endpoints at topology compilation. This is a design-time constraint to surface in review of such features, not a change in this slice.

### Tests

Most existing tests adapt mechanically (`Body` assertions compare via `.ToArray()`/`.Span.SequenceEqual`, constructor calls keep compiling through the implicit conversion). The aliasing test should assert `message.Body` equals the source memory region for `copyBody: false` (e.g. via `MemoryMarshal.TryGetArray` or unsafe reference comparison of `Body.Span` and the source span) and differs for the default.

The `CloudEventEnvelope` equality change does **not** adapt mechanically — it is a semantic change to an `[Equatable]` record struct and needs explicit coverage. `CloudEventEnvelopeTests` already pins this (`Equals_ComparesDataByItem`, `Equals_DetectsItemDifferences`, `GetHashCode_CreatesSameHashCodeForEqualEnvelopes`, and a `byte[]? data` helper); update it so equal `Data` content held in **distinct backing arrays** still compares equal and produces equal hash codes, and differing content compares unequal. This guards against a custom comparer that accidentally compares by reference/range instead of content. Also review `Utf8JsonPayloadCodecTests` and `CloudEventMessageSerializerTests` for `Decode` call sites and `Data` assertions affected by the `byte[]` → `ReadOnlyMemory<byte>` switch.
