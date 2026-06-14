using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface ITopologyProvisioner
{
    Task ProvisionAsync(CancellationToken cancellationToken = default);
}
