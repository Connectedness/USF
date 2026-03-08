## Rationale

USF needs a first message publishing slice that keeps the public programming model transport-agnostic while allowing `Usf.Transport.RabbitMq` to provide the first concrete implementation. The design should keep `Usf.Core` focused on abstractions and immutable runtime models, allow applications to define message routing and broker topology in the composition root, and preserve efficient handling of value-type messages by using generic serializer and target APIs that avoid boxing on the publish hot path. Observability should be included from the beginning so publish behavior and topology provisioning can be inspected in production.

## Acceptance Criteria

- [x] `Usf.Core` contains the initial message publishing abstractions centered around `IMessagePublisher`, `IMessageTopology`, `ITargetRegistry`, `ITopologyProvisioner`, `Target`, `Target<T>`, `SerializedMessage`, `IMessageSerializer` with a generic `SerializeAsync<T>(T message, CancellationToken cancellationToken = default)` method, and dedicated exception types for missing targets, target/message mismatches, serialization failures, and topology validation failures.
- [x] The publish flow supports both explicit targets and topology-based resolution by exact message `Type`, rejects `null` messages, and uses dedicated exception types for missing targets, invalid target/message combinations, and serialization failures.
- [x] The runtime path for publishing struct messages uses the generic serializer and generic target APIs so message values do not need to be boxed during serialization and typed target execution.
- [x] Target instances encapsulate serialization and transport dispatch so `IMessagePublisher` remains unaware of transport-specific broker concepts.
- [x] `Usf.Transport.RabbitMq` provides RabbitMQ-specific targets plus topology definitions and provisioning for exchanges, queues, and bindings, including support for no declaration, passive declaration, and active declaration.
- [x] Applications can define the publishing topology in the composition root, including per-message target mappings, serializer selection, RabbitMQ entity definitions, and named target registration for explicit target retrieval.
- [x] Startup validation rejects duplicate message routes, named target collisions, missing serializers, invalid RabbitMQ entity references, and inconsistent declare configurations before the application can publish messages, and reports all discovered validation failures through `MessageTopologyValidationException` with a non-empty, deterministic `ValidationErrors` collection.
- [x] Observability includes activities for publish and topology provisioning plus metrics for publish attempts, publish failures, publish duration, topology provisioning attempts, topology provisioning failures, and topology provisioning duration, tagged with at least `message.type`, `target.name`, `transport.name`, and `outcome` where applicable, without recording serialized message bodies in telemetry.
- [x] An integration test verifies RabbitMQ publishing end-to-end by spinning up RabbitMQ with Testcontainers and asserting the expected message body, routing outcome, and at least one mapped metadata value such as a header or correlation identifier.
- [x] Automated tests are written.

## Technical Details

The initial implementation should compile the application's publishing configuration into immutable runtime objects during startup. `IMessageTopology` should expose lookup by message `Type` for the default publish path, use exact type matching in the first version to keep resolution deterministic and efficient, and remain the single topology abstraction for a configured message broker even when that broker is used for both publishing and consuming messages. The topology should be built once, validated at startup, and then used as a singleton lookup during message publishing. `ITopologyProvisioner` should define the startup hook that transport packages use to perform active declarations or passive checks before the application begins normal work, and startup should execute all registered provisioners that contribute to the configured broker topology. Provisioning should be safe to run once during each application startup and fail fast when an existing broker definition is incompatible with the configured topology.

A concrete sketch of the central core abstractions could look like this:

```csharp
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessagePublisher
{
    Task PublishMessageAsync<T>(T message, Target? target = null, CancellationToken cancellationToken = default);
}

public interface IMessageTopology
{
    Target GetRequiredTarget(Type messageType);
    Target<T> GetRequiredTarget<T>();
    bool TryGetTarget(Type messageType, out Target? target);
}

public interface ITargetRegistry
{
    Target GetRequiredTarget(string name);
    Target<T> GetRequiredTarget<T>(string name);
    bool TryGetTarget(string name, out Target? target);
}

public interface ITopologyProvisioner
{
    Task ProvisionAsync(CancellationToken cancellationToken = default);
}

public interface IMessageSerializer
{
    Task<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default);
}

public readonly record struct SerializedMessage(
    byte[] Body,
    string? ContentType,
    string? ContentEncoding,
    IReadOnlyDictionary<string, string?> Headers,
    string? MessageId,
    string? CorrelationId);

public sealed class MessageTargetNotFoundException : Exception
{
    public MessageTargetNotFoundException(Type messageType)
        : base($"No target is registered for message type '{messageType}'.")
    {
    }

    public MessageTargetNotFoundException(string targetName)
        : base($"No target is registered with name '{targetName}'.")
    {
    }
}

public sealed class MessageTargetTypeMismatchException : Exception
{
    public MessageTargetTypeMismatchException(string targetName, Type actualMessageType, Type expectedMessageType)
        : base($"Target '{targetName}' cannot publish messages of type '{actualMessageType}'. Expected '{expectedMessageType}'.")
    {
    }
}

public sealed class MessageSerializationException : Exception
{
    public MessageSerializationException(Type messageType, Exception innerException)
        : base($"Serialization failed for message type '{messageType}'.", innerException)
    {
    }
}

public sealed class MessageTopologyValidationException : Exception
{
    public MessageTopologyValidationException(IReadOnlyList<string> validationErrors)
        : base("Message topology validation failed.")
    {
        ArgumentNullException.ThrowIfNull(validationErrors);
        if (validationErrors.Count == 0)
        {
            throw new ArgumentException("At least one validation error must be provided.", nameof(validationErrors));
        }

        ValidationErrors = validationErrors;
    }

    public IReadOnlyList<string> ValidationErrors { get; }
}

public abstract class Target
{
    protected Target(Type messageType, string name)
    {
        MessageType = messageType;
        Name = name;
    }

    public Type MessageType { get; }
    public string Name { get; }

    public abstract Task PublishUntypedAsync(object message, CancellationToken cancellationToken = default);
}

public abstract class Target<T> : Target
{
    protected Target(string name, IMessageSerializer serializer)
        : base(typeof(T), name)
    {
        Serializer = serializer;
    }

    protected IMessageSerializer Serializer { get; }

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        SerializedMessage serializedMessage;

        try
        {
            serializedMessage = await Serializer.SerializeAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException && exception is not MessageSerializationException)
        {
            throw new MessageSerializationException(typeof(T), exception);
        }

        await DispatchAsync(message, serializedMessage, cancellationToken).ConfigureAwait(false);
    }

    public sealed override Task PublishUntypedAsync(object message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message is not T typedMessage)
        {
            throw new MessageTargetTypeMismatchException(Name, message.GetType(), typeof(T));
        }

        return PublishAsync(typedMessage, cancellationToken);
    }

    protected abstract Task DispatchAsync(T message, SerializedMessage serializedMessage, CancellationToken cancellationToken);
}

public static class MessagePublishingDiagnostics
{
    public static readonly ActivitySource ActivitySource = new("Usf.Messaging");
    public static readonly Meter Meter = new("Usf.Messaging");
    public const string MessageTypeTagName = "message.type";
    public const string TargetNameTagName = "target.name";
    public const string TransportNameTagName = "transport.name";
    public const string OutcomeTagName = "outcome";
    public static readonly Counter<long> PublishAttempts = Meter.CreateCounter<long>("usf.messaging.publish.attempts");
    public static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>("usf.messaging.publish.failures");
    public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>("usf.messaging.publish.duration", unit: "ms");
    public static readonly Counter<long> TopologyProvisioningAttempts = Meter.CreateCounter<long>("usf.messaging.topology.provisioning.attempts");
    public static readonly Counter<long> TopologyProvisioningFailures = Meter.CreateCounter<long>("usf.messaging.topology.provisioning.failures");
    public static readonly Histogram<double> TopologyProvisioningDuration = Meter.CreateHistogram<double>("usf.messaging.topology.provisioning.duration", unit: "ms");
}

```

`Target` should be an abstract non-generic base class used at abstraction boundaries such as topology dictionaries and named target registries. `Target<T>` should derive from it and provide the typed execution path for publishing a specific message type. The generic publisher method should use that typed path whenever possible so value-type messages are not boxed while being passed to the target or serialized. The non-generic base should still validate runtime compatibility and provide a fallback bridge needed for storage, lookup, and diagnostics. Invalid target/message combinations should raise `MessageTargetTypeMismatchException` rather than a general-purpose exception type.

A corresponding publisher implementation can then prefer the typed path while still supporting the non-generic `Target` parameter of the public API:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public sealed class MessagePublisher : IMessagePublisher
{
    private readonly IMessageTopology _messageTopology;

    public MessagePublisher(IMessageTopology messageTopology)
    {
        _messageTopology = messageTopology;
    }

    public Task PublishMessageAsync<T>(T message, Target? target = null, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        Target resolvedTarget = target ?? _messageTopology.GetRequiredTarget<T>();

        if (resolvedTarget is Target<T> typedTarget)
        {
            return typedTarget.PublishAsync(message, cancellationToken);
        }

        return resolvedTarget.PublishUntypedAsync(message!, cancellationToken);
    }
}

```

`IMessageSerializer` should remain non-generic as an abstraction, but it should expose a generic `SerializeAsync<T>` method so serializers can handle structs and classes efficiently. Targets should own the serializer that applies to their message route, which keeps serializer selection out of the hot path of `IMessagePublisher` and makes each compiled target a complete executable route. `SerializedMessage` should carry the message body and serialization metadata needed by transports, including content type, content encoding, extensible headers, message identity, and correlation data. The headers should represent transport-agnostic logical metadata rather than a direct projection of any single broker's native header model.

`IMessagePublisher` should stay minimal. When an explicit target is supplied, the publisher should use it directly. Otherwise it should resolve the target from `IMessageTopology` by the exact message type. Missing targets should surface as `MessageTargetNotFoundException`, `null` messages should surface as `ArgumentNullException`, and serialization failures should surface as `MessageSerializationException`. Cancellation should propagate unchanged and must not be wrapped in `MessageSerializationException` or another framework-specific exception. The publisher should not know about exchanges, queues, routing keys, subjects, or other transport concepts. It should only coordinate resolution, validation, and delegation to the target.

Named explicit targets should be supported in the first version through a dedicated registry abstraction rather than by requiring callers to construct targets themselves. The composition root should allow assigning stable names to targets and retrieving them through dependency injection. Missing named targets should surface as `MessageTargetNotFoundException`. This keeps explicit publish routes available for advanced callers without weakening topology ownership or transport encapsulation. Named target lookups should participate in the same validation rules as type-based topology lookups so collisions and mismatches are caught deterministically.

RabbitMQ-specific concepts should live only in `Usf.Transport.RabbitMq`. That package should define the infrastructure model for exchanges, queues, and bindings, plus the corresponding declaration mode for each entity. A RabbitMQ implementation of `ITopologyProvisioner` should perform topology provisioning during startup, not during each publish call, so passive checks and active declarations fail fast and do not slow down the steady-state publish path. Provisioning should remain idempotent for a normal startup sequence and fail immediately if existing exchanges, queues, or bindings are incompatible with the configured definitions. The RabbitMQ target implementation should serialize the message, translate serialization metadata into RabbitMQ message properties and headers, and publish via the configured exchange and routing key.

The composition root API should separate infrastructure definition from message route definition. One part of the API should define RabbitMQ entities and their declare behavior. Another part should map individual message types to typed targets and serializers, and optionally assign target names. Startup validation should reject duplicate message type routes, named target collisions, missing serializers, invalid RabbitMQ entity references, and inconsistent declare settings by throwing `MessageTopologyValidationException` before the application can start publishing messages, and that exception should report all discovered validation failures together in a non-empty, deterministic collection. This separation allows the same core model to support additional transports later without leaking RabbitMQ-specific vocabulary into `Usf.Core`.

Observability should be part of the first slice rather than an afterthought. The implementation should create activities for publish and topology provisioning operations and tag them with `message.type`, `target.name`, `transport.name`, and `outcome` where applicable. It should also emit counters for publish attempts, publish failures, topology provisioning attempts, and topology provisioning failures, plus histograms for publish duration and topology provisioning duration, using the same tag keys where those dimensions apply. Telemetry should not capture serialized message bodies. This should integrate with standard .NET observability primitives so applications can connect logging, tracing, and metrics without transport-specific coupling leaking into calling code.
