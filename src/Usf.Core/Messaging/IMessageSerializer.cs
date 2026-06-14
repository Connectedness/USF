using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message into a CloudEvents envelope.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <param name="metadata">The call-site-owned CloudEvents attributes.</param>
    /// <param name="type">
    /// The already-resolved CloudEvents <c>type</c> discriminator.
    /// </param>
    /// <param name="dataSchema">The already-resolved CloudEvents <c>dataschema</c> value, if any.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask<CloudEventEnvelope> SerializeAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    );

    ValueTask<object?> DeserializeAsync(
        CloudEventEnvelope envelope,
        Type messageType,
        CancellationToken cancellationToken = default
    );
}
