using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class RabbitMqIntegrationSerializer : IMessageSerializer
{
    public ValueTask<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        var typedMessage = message.Should().BeOfType<RabbitMqPublishMessage>().Subject;
        var serializedMessage = new SerializedMessage(
            Encoding.UTF8.GetBytes($"{{\"Id\":{typedMessage.Id},\"Name\":\"{typedMessage.Name}\"}}"),
            "application/json",
            "utf-8",
            new ReadOnlyDictionary<string, string?>(
                new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    ["tenant"] = $"tenant-{typedMessage.Name}"
                }
            ),
            $"msg-{typedMessage.Id}",
            $"corr-{typedMessage.Id}"
        );
        return new ValueTask<SerializedMessage>(serializedMessage);
    }
}
