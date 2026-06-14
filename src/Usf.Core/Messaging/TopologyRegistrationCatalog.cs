using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Usf.Core.Messaging;

/// <summary>
/// Tracks the registered topology names. Topology names form a single ordinal string namespace that matches the
/// one-connection/client ownership boundary, so registering the same name twice fails even when
/// one registration is publish-only and the other is consume-only.
/// </summary>
public sealed class TopologyRegistrationCatalog
{
    private readonly List<string> _names = [];
    private readonly HashSet<string> _namesSet = new (StringComparer.Ordinal);

    public IReadOnlyCollection<string> Names => new ReadOnlyCollection<string>(_names);

    public void Add(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_namesSet.Add(name))
        {
            throw new InvalidOperationException(
                $"Topology '{name}' is already registered. Registered topologies: {FormatNames(_names)}."
            );
        }

        _names.Add(name);
    }

    public bool Contains(string name)
    {
        return _namesSet.Contains(name);
    }

    public static string FormatNames(IEnumerable<string> names)
    {
        var values = names
           .OrderBy(static value => value, StringComparer.Ordinal)
           .ToArray();

        return values.Length == 0 ? "(none)" : string.Join(", ", values);
    }
}
