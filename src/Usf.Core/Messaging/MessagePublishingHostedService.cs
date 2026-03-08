using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Usf.Core.Messaging;

public sealed class MessagePublishingHostedService : IHostedService
{
    private readonly IMessageTopology _messageTopology;
    private readonly IEnumerable<ITopologyProvisioner> _topologyProvisioners;

    public MessagePublishingHostedService(
        IMessageTopology messageTopology,
        IEnumerable<ITopologyProvisioner> topologyProvisioners
    )
    {
        _messageTopology = messageTopology ?? throw new ArgumentNullException(nameof(messageTopology));
        _topologyProvisioners = topologyProvisioners ?? throw new ArgumentNullException(nameof(topologyProvisioners));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _ = _messageTopology;

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
