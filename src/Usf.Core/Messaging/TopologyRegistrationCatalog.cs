using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Usf.Core.Messaging;

/// <summary>
/// Tracks the registered topology names. Topology names form a single namespace keyed by <see cref="TopologyName" />
/// that matches the one-connection/client ownership boundary, so registering the same name twice fails even when
/// one registration is publish-only and the other is consume-only.
/// </summary>
public sealed class TopologyRegistrationCatalog
{
    private readonly List<TopologyName> _names = [];
    private readonly HashSet<TopologyName> _namesSet = [];

    public IReadOnlyCollection<TopologyName> Names => new ReadOnlyCollection<TopologyName>(_names);

    public void Add(TopologyName name)
    {
        if (!_namesSet.Add(name))
        {
            throw new InvalidOperationException(
                $"Topology '{name.Value}' is already registered. Registered topologies: {FormatNames(_names)}."
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
