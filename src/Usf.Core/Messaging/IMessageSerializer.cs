using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessageSerializer
{
    Task<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default);
}
