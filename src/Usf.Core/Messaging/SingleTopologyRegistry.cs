using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public abstract class SingleTopologyRegistry<TTopology>
{
    private readonly TTopology _topology;

    protected SingleTopologyRegistry(TTopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        Names = [TopologyName.Default];
    }

    protected abstract string Direction { get; }

    public IReadOnlyCollection<TopologyName> Names { get; }

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
        if (name == TopologyName.Default)
        {
            topology = _topology;
            return true;
        }

        topology = default;
        return false;
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
