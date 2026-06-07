using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Usf.Core.Messaging;

/// <summary>
/// Discovers the registered <see cref="ITopologyRuntime" /> instances and drives their host-lifetime start/stop
/// behavior. It is registered after <see cref="TopologyProvisioningHostedService" /> so that broker resources are
/// provisioned before any topology runtime starts. Runtimes are stopped in reverse start order during host
/// shutdown so each transport can perform its own graceful drain.
/// </summary>
public sealed class TopologyRuntimeHostedService : IHostedService
{
    private readonly IReadOnlyList<ITopologyRuntime> _runtimes;

    public TopologyRuntimeHostedService(IEnumerable<ITopologyRuntime> runtimes)
    {
        if (runtimes is null)
        {
            throw new ArgumentNullException(nameof(runtimes));
        }

        _runtimes = runtimes.ToArray();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var runtime in _runtimes)
        {
            await runtime.StartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        for (var index = _runtimes.Count - 1; index >= 0; index--)
        {
            await _runtimes[index].StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
