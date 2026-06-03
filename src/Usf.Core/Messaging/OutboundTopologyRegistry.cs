using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public sealed class OutboundTopologyRegistry : IOutboundTopologyRegistry
{
    private readonly OutboundTopologyRegistrationCatalog _catalog;
    private readonly IServiceProvider _serviceProvider;

    public OutboundTopologyRegistry(
        IServiceProvider serviceProvider,
        OutboundTopologyRegistrationCatalog catalog
    )
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    public IReadOnlyCollection<TopologyName> Names => _catalog.Names;

    public IOutboundTopology GetRequiredTopology(TopologyName name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"Outbound topology '{name.Value}' is not registered. Registered outbound topologies: {OutboundTopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out IOutboundTopology? topology)
    {
        if (!_catalog.Contains(name))
        {
            topology = null;
            return false;
        }

        topology = _serviceProvider.GetRequiredKeyedService<IOutboundTopology>(name);
        return true;
    }
}
