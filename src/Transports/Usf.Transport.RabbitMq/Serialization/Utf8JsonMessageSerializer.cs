using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq.Serialization;

public sealed class Utf8JsonMessageSerializer : IMessageSerializer
{
    public Task<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var json = JsonSerializer.Serialize(message);
        SerializedMessage serializedMessage = new (
            Encoding.UTF8.GetBytes(json),
            "application/json",
            "utf-8",
            new ReadOnlyDictionary<string, string?>(new Dictionary<string, string?>(StringComparer.Ordinal)),
            null,
            null
        );
        return Task.FromResult(serializedMessage);
    }
}
