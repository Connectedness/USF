using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

/// <summary>
/// Creates strongly typed message handler invocation delegates.
/// </summary>
public static class MessageHandlerInvocation
{
    /// <summary>
    /// Creates a delegate that resolves <typeparamref name="THandler" /> from the per-delivery service scope and
    /// invokes it with the deserialized <typeparamref name="TMessage" />.
    /// </summary>
    /// <remarks>
    /// The supplied context must contain the deserialized message in
    /// <see cref="IncomingMessageContext.Message" /> and its <see cref="IncomingMessageContext.Services" /> must be
    /// the per-delivery service scope.
    /// </remarks>
    public static MessageDelegate Create<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        return static context => context.Services
           .GetRequiredService<THandler>()
           .HandleAsync((TMessage) context.Message!, context, context.CancellationToken);
    }
}
