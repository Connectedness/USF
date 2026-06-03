using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Usf.Core.Messaging;

public sealed class OutboundTopologyRegistrationCatalog
{
    private readonly List<TopologyName> _names = [];
    private readonly HashSet<TopologyName> _namesSet = [];

    public IReadOnlyCollection<TopologyName> Names => new ReadOnlyCollection<TopologyName>(_names);

    public void Add(TopologyName name)
    {
        if (!_namesSet.Add(name))
        {
            throw new InvalidOperationException(
                $"Outbound topology '{name.Value}' is already registered. Registered outbound topologies: {FormatNames(_names)}."
            );
        }

        _names.Add(name);
    }

    public bool Contains(TopologyName name)
    {
        return _namesSet.Contains(name);
    }

    public static string FormatNames(IEnumerable<TopologyName> names)
    {
        var values = names
           .Select(static name => name.Value)
           .OrderBy(static value => value, StringComparer.Ordinal)
           .ToArray();

        return values.Length == 0 ? "(none)" : string.Join(", ", values);
    }
}
