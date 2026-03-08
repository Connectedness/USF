using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.TestSupport;

public sealed class RecordingSerializer : IMessageSerializer
{
    private readonly SerializedMessage _serializedMessage;

    public RecordingSerializer(SerializedMessage serializedMessage)
    {
        _serializedMessage = serializedMessage;
    }

    public List<object> Messages { get; } = [];

    public Task<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        Messages.Add(message!);
        return Task.FromResult(_serializedMessage);
    }
}
