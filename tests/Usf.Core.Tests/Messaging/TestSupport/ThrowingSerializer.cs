using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class ThrowingSerializer : IMessageSerializer
{
    private readonly Exception _exception;

    public ThrowingSerializer(Exception exception)
    {
        _exception = exception;
    }

    public ValueTask<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        throw _exception;
    }
}
