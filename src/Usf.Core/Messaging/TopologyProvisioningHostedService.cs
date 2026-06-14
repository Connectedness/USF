using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Usf.Core.Messaging;

public sealed class TopologyProvisioningHostedService : IHostedService
{
    private readonly IEnumerable<ITopologyProvisioner> _topologyProvisioners;

    public TopologyProvisioningHostedService(IEnumerable<ITopologyProvisioner> topologyProvisioners)
    {
        _topologyProvisioners = topologyProvisioners ??
                                throw new ArgumentNullException(nameof(topologyProvisioners));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var topologyProvisioner in _topologyProvisioners)
        {
            await topologyProvisioner.ProvisionAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
