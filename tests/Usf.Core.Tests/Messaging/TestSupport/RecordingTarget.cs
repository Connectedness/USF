using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.TestSupport;

public sealed class RecordingTarget<TMessage> : Target<TMessage>
{
    public RecordingTarget(string name, IMessageSerializer serializer)
        : base(name, "test", serializer) { }

    public List<TMessage> Messages { get; } = [];

    public List<SerializedMessage> SerializedMessages { get; } = [];

    protected override Task DispatchAsync(
        TMessage message,
        SerializedMessage serializedMessage,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message);
        SerializedMessages.Add(serializedMessage);
        return Task.CompletedTask;
    }
}
