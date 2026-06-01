using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public abstract class OutboundTarget
{
    protected OutboundTarget(string name, string transportName)
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
    }

    public virtual Type? MessageType => null;

    public string Name { get; }

    public string TransportName { get; }

    public abstract Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    );
}

public abstract class OutboundTarget<T> : OutboundTarget
{
    protected OutboundTarget(string name, string transportName, IMessageSerializer serializer)
        : base(name, transportName)
    {
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    public sealed override Type MessageType => typeof(T);

    protected IMessageSerializer Serializer { get; }

    public Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        if (message is not ICloudEvent cloudEvent)
        {
            throw new CloudEventMetadataException(
                "id",
                "Implement ICloudEvent or derive from BaseCloudEvent, or call PublishAsync with explicit CloudEventMetadata."
            );
        }

        var metadata = CloudEventMetadata.From(cloudEvent);
        return PublishAsync(message, in metadata, cancellationToken);
    }

    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        CancellationToken cancellationToken = default
    )
    {
        return PublishCoreAsync(message, metadata, type: null, cancellationToken);
    }

    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string type,
        CancellationToken cancellationToken
    )
    {
        return PublishCoreAsync(message, metadata, type, cancellationToken);
    }

    private async Task PublishCoreAsync(
        T message,
        CloudEventMetadata metadata,
        string? type,
        CancellationToken cancellationToken
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        CloudEventEnvelope envelope;

        try
        {
            envelope = await Serializer.SerializeAsync(message, in metadata, type, cancellationToken)
               .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException &&
                                          exception is not MessageSerializationException)
        {
            throw new MessageSerializationException(typeof(T), exception);
        }

        await PublishTypedCloudEventAsync(message, envelope, cancellationToken).ConfigureAwait(false);
    }

    protected abstract Task PublishTypedCloudEventAsync(
        T message,
        CloudEventEnvelope envelope,
        CancellationToken cancellationToken
    );
}
