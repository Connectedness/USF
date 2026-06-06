using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Usf.Core.Messaging;

public sealed class OutboundTopologyHostedService : IHostedService
{
    private readonly TopologyProvisioningHostedService _inner;

    public OutboundTopologyHostedService(IEnumerable<IOutboundTopologyProvisioner> topologyProvisioners)
    {
        _inner = new TopologyProvisioningHostedService(topologyProvisioners);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _inner.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
