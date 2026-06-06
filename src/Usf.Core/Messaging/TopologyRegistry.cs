using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public abstract class TopologyRegistry<TTopology>
    where TTopology : notnull
{
    private readonly TopologyRegistrationCatalog _catalog;
    private readonly IServiceProvider _serviceProvider;

    protected TopologyRegistry(
        IServiceProvider serviceProvider,
        TopologyRegistrationCatalog catalog
    )
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    protected abstract string Direction { get; }

    public IReadOnlyCollection<TopologyName> Names => _catalog.Names;

    public TTopology GetRequiredTopology(TopologyName name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"{ToSentenceCase(Direction)} topology '{name.Value}' is not registered. Registered {Direction} topologies: {TopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    public bool TryGetTopology(TopologyName name, out TTopology? topology)
    {
        if (!_catalog.Contains(name))
        {
            topology = default;
            return false;
        }

        topology = _serviceProvider.GetRequiredKeyedService<TTopology>(name);
        return true;
    }

    protected static string ToSentenceCase(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(value));
        }

        return char.ToUpperInvariant(value[0]) + value.Substring(1);
    }
}
