using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public sealed class TopologyRegistry : ITopologyRegistry
{
    private readonly TopologyRegistrationCatalog _catalog;
    private readonly IServiceProvider _serviceProvider;

    public TopologyRegistry(IServiceProvider serviceProvider, TopologyRegistrationCatalog catalog)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    public IReadOnlyCollection<TopologyName> Names => _catalog.Names;

    public ITopology GetRequiredTopology(TopologyName name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"Topology '{name.Value}' is not registered. Registered topologies: {TopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out ITopology? topology)
    {
        if (!_catalog.Contains(name))
        {
            topology = default;
            return false;
        }

        topology = _serviceProvider.GetRequiredKeyedService<ITopology>(name);
        return true;
    }
}
