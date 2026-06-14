using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessageHandler<in TMessage>
{
    Task HandleAsync(TMessage message, IncomingMessageContext context, CancellationToken cancellationToken);
}
