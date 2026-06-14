using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessageMiddleware
{
    Task InvokeAsync(IncomingMessageContext context, MessageDelegate next);
}
