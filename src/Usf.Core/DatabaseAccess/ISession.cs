using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.DatabaseAccess;

public interface ISession : IAsyncDisposable
{
    Task SaveChangesAsync(CancellationToken cancellationToken = default);
}
