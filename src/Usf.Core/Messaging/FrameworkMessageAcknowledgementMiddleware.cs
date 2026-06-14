using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public sealed class FrameworkMessageAcknowledgementMiddleware : IMessageMiddleware
{
    public async Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (next is null)
        {
            throw new ArgumentNullException(nameof(next));
        }

        try
        {
            await next(context).ConfigureAwait(false);

            if (context.Endpoint.AckMode == MessageAckMode.Auto)
            {
                await context.Acknowledgement.AckAsync(context.CancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            if (context.Endpoint.AckMode == MessageAckMode.Auto)
            {
                await context.Acknowledgement.NackAsync(requeue: true, CancellationToken.None)
                   .ConfigureAwait(false);
            }

            throw;
        }
        catch
        {
            if (context.Endpoint.AckMode == MessageAckMode.Auto)
            {
                await context.Acknowledgement.NackAsync(requeue: false, CancellationToken.None)
                   .ConfigureAwait(false);
            }

            throw;
        }
    }
}
