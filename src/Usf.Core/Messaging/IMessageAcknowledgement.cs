using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessageAcknowledgement
{
    Task AckAsync(CancellationToken cancellationToken = default);

    Task NackAsync(bool requeue, CancellationToken cancellationToken = default);
}
