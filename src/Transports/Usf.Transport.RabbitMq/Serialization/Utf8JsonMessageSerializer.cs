using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq.Serialization;

public sealed class Utf8JsonMessageSerializer : IMessageSerializer
{
    public ValueTask<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var utf8Bytes = JsonSerializer.SerializeToUtf8Bytes(message);
        SerializedMessage serializedMessage = new (
            utf8Bytes,
            "application/json",
            "utf-8",
            new Dictionary<string, string?>(StringComparer.Ordinal),
            null,
            null
        );

        return new ValueTask<SerializedMessage>(serializedMessage);
    }
}
