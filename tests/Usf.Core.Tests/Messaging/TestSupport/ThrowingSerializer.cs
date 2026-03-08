using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.TestSupport;

public sealed class ThrowingSerializer : IMessageSerializer
{
    private readonly Exception _exception;

    public ThrowingSerializer(Exception exception)
    {
        _exception = exception;
    }

    public Task<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        return Task.FromException<SerializedMessage>(_exception);
    }
}
