using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public abstract class OutboundTarget
{
    protected OutboundTarget(string name, string transportName, TopologyName? topologyName = null)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (string.IsNullOrWhiteSpace(transportName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(transportName));
        }

        Name = name;
        TransportName = transportName;
        TopologyName = topologyName ?? TopologyName.Default;
    }

    public virtual Type? MessageType => null;

    public string Name { get; }

    public TopologyName TopologyName { get; }

    public string TransportName { get; }

    public virtual string GetDiagnosticMessageTypeName(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        return runtimeMessageType.FullName ?? runtimeMessageType.Name;
    }

    public abstract Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    );
}

public abstract class OutboundTarget<T> : OutboundTarget
{
    protected OutboundTarget(
        string name,
        string transportName,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        TopologyName? topologyName = null
    )
        : base(name, transportName, topologyName)
    {
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
    }

    public sealed override Type MessageType => typeof(T);

    protected IMessageSerializer Serializer { get; }

    protected IMessageContractRegistry MessageContractRegistry { get; }

    public sealed override string GetDiagnosticMessageTypeName(Type runtimeMessageType)
    {
        return GetRequiredDiscriminator(runtimeMessageType);
    }

    public string GetRequiredDiscriminator(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        try
        {
            return MessageContractRegistry.GetDiscriminator(runtimeMessageType);
        }
        catch (MessageContractNotRegisteredException exception)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Type,
                $"Register the runtime message type '{exception.MessageType}' with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
            );
        }
    }

    public string? GetDataSchema(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        return MessageContractRegistry.GetDataSchema(runtimeMessageType);
    }

    public Task PublishAsync(
        T message,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        if (message is not ICloudEvent cloudEvent)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Id,
                "Implement ICloudEvent or derive from BaseCloudEvent, or call PublishAsync with explicit CloudEventMetadata."
            );
        }

        var metadata = CloudEventMetadata.From(cloudEvent);
        return PublishAsync(message, in metadata, routingKey, cancellationToken);
    }

    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        return PublishCoreAsync(message, metadata, type: null, dataSchema: null, routingKey, cancellationToken);
    }

    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string type,
        string? dataSchema,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        return PublishCoreAsync(message, metadata, type, dataSchema, routingKey, cancellationToken);
    }

    private async Task PublishCoreAsync(
        T message,
        CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var runtimeType = message.GetType();
        var resolvedType = type ?? GetRequiredDiscriminator(runtimeType);
        var resolvedDataSchema = dataSchema ?? GetDataSchema(runtimeType);
        CloudEventEnvelope envelope;

        try
        {
            envelope = await Serializer.SerializeAsync(
                    message,
                    in metadata,
                    resolvedType,
                    resolvedDataSchema,
                    cancellationToken
                )
               .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException &&
                                          exception is not MessageSerializationException)
        {
            throw new MessageSerializationException(runtimeType, exception);
        }

        await PublishTypedCloudEventAsync(message, envelope, routingKey, cancellationToken).ConfigureAwait(false);
    }

    protected abstract Task PublishTypedCloudEventAsync(
        T message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    );
}
