using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public sealed class MessageHandlerInvoker
{
    private static readonly MethodInfo InvokeCoreMethod = typeof(MessageHandlerInvoker)
       .GetMethod(nameof(InvokeCoreAsync), BindingFlags.Static | BindingFlags.NonPublic)!;

    public Task InvokeAsync(IncomingMessageContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (context.Message is null)
        {
            throw new InvalidOperationException("The inbound message has not been deserialized.");
        }

        var handlerInterfaceType = typeof(IMessageHandler<>).MakeGenericType(context.Endpoint.MessageType);
        var handler = context.Services.GetRequiredService(handlerInterfaceType);
        var closedMethod = InvokeCoreMethod.MakeGenericMethod(context.Endpoint.MessageType);
        return (Task) closedMethod.Invoke(
            null,
            [handler, context.Message, context, context.CancellationToken]
        )!;
    }

    private static Task InvokeCoreAsync<TMessage>(
        IMessageHandler<TMessage> handler,
        object message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        return handler.HandleAsync((TMessage) message, context, cancellationToken);
    }
}
