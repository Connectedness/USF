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

    public IReadOnlyCollection<string> Names => _catalog.Names;

    public Topology GetRequiredTopology(string name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"Topology '{name}' is not registered. Registered topologies: {TopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(string name, out Topology? topology)
    {
        if (!_catalog.Contains(name))
        {
            topology = default;
            return false;
        }

        topology = _serviceProvider.GetRequiredKeyedService<Topology>(name);
        return true;
    }
}
