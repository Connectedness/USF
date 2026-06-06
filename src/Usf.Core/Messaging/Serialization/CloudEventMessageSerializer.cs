using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging.Serialization;

/// <summary>
/// Serializes messages as CloudEvents v1.0 envelopes in binary content mode.
/// </summary>
/// <remarks>
/// The serializer assembles transport-neutral attributes. A transport applies its protocol binding. The
/// serializer never generates retry-sensitive id or time attributes.
/// </remarks>
public sealed class CloudEventMessageSerializer : IMessageSerializer
{
    private readonly CloudEventsOptions _options;
    private readonly IPayloadCodec _payloadCodec;

    public CloudEventMessageSerializer(
        IPayloadCodec payloadCodec,
        CloudEventsOptions options
    )
    {
        _payloadCodec = payloadCodec ?? throw new ArgumentNullException(nameof(payloadCodec));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (metadata.Id == Guid.Empty)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Id,
                "Implement ICloudEvent or derive from BaseCloudEvent, or pass CloudEventMetadata with a non-empty Id."
            );
        }

        if (metadata.Time == default)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Time,
                "Implement ICloudEvent or derive from BaseCloudEvent, or pass CloudEventMetadata with a construction-time Time value."
            );
        }

        var source = CloudEventsOptionsValidation.GetRequiredSource(metadata.Source ?? _options.Source);
        var resolvedType = GetRequiredType(type);
        var payload = _payloadCodec.Encode(message);

        if (string.IsNullOrWhiteSpace(payload.DataContentType))
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.DataContentType,
                "Configure the payload codec to return a non-empty data content type."
            );
        }

        CloudEventEnvelope envelope = new (
            "1.0",
            metadata.Id.ToString("D"),
            source,
            resolvedType,
            metadata.Time,
            metadata.Subject,
            payload.DataContentType,
            dataSchema,
            payload.Data
        );

        return new ValueTask<CloudEventEnvelope>(envelope);
    }

    public ValueTask<object?> DeserializeAsync(
        CloudEventEnvelope envelope,
        Type messageType,
        CancellationToken cancellationToken = default
    )
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        try
        {
            return new ValueTask<object?>(_payloadCodec.Decode(envelope.Data, messageType));
        }
        catch (Exception exception)
        {
            throw new MessageDeserializationException(messageType, exception);
        }
    }

    private static string GetRequiredType(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Type,
                "Resolve a non-empty CloudEvents type discriminator before serializing the message."
            );
        }

        return type!;
    }
}
