using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class RabbitMqIntegrationSerializer : IMessageSerializer
{
    private readonly Utf8JsonMessageSerializer _innerSerializer;

    public RabbitMqIntegrationSerializer(Utf8JsonMessageSerializer innerSerializer)
    {
        _innerSerializer = innerSerializer ?? throw new ArgumentNullException(nameof(innerSerializer));
    }

    public async ValueTask<SerializedMessage> SerializeAsync<T>(
        T message,
        CancellationToken cancellationToken = default
    )
    {
        var typedMessage = message.Should().BeOfType<RabbitMqPublishMessage>().Subject;
        var serializedMessage = await _innerSerializer.SerializeAsync(message, cancellationToken).ConfigureAwait(false);
        Dictionary<string, string?> headers = new (StringComparer.Ordinal);

        foreach (var header in serializedMessage.Headers)
        {
            headers[header.Key] = header.Value;
        }

        headers["tenant"] = $"tenant-{typedMessage.Name}";
        return serializedMessage with
        {
            Headers = new ReadOnlyDictionary<string, string?>(headers),
            MessageId = $"msg-{typedMessage.Id}",
            CorrelationId = $"corr-{typedMessage.Id}"
        };
    }
}
