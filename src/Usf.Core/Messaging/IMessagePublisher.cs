using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessagePublisher
{
    Task PublishMessageAsync<T>(T message, Target? target = null, CancellationToken cancellationToken = default);
}
