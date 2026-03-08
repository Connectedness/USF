using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public abstract class Target<T> : Target
{
    protected Target(string name, string transportName, IMessageSerializer serializer)
        : base(typeof(T), name, transportName)
    {
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    protected IMessageSerializer Serializer { get; }

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        SerializedMessage serializedMessage;

        try
        {
            serializedMessage = await Serializer.SerializeAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException &&
                                          exception is not MessageSerializationException)
        {
            throw new MessageSerializationException(typeof(T), exception);
        }

        await DispatchAsync(message, serializedMessage, cancellationToken).ConfigureAwait(false);
    }

    public sealed override Task PublishUntypedAsync(object message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (message is not T typedMessage)
        {
            throw new MessageTargetTypeMismatchException(Name, message.GetType(), typeof(T));
        }

        return PublishAsync(typedMessage, cancellationToken);
    }

    protected abstract Task DispatchAsync(
        T message,
        SerializedMessage serializedMessage,
        CancellationToken cancellationToken
    );
}
