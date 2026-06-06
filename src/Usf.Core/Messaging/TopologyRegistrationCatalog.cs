using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Usf.Core.Messaging;

public abstract class TopologyRegistrationCatalog
{
    private readonly List<TopologyName> _names = [];
    private readonly HashSet<TopologyName> _namesSet = [];

    protected abstract string Direction { get; }

    public IReadOnlyCollection<TopologyName> Names => new ReadOnlyCollection<TopologyName>(_names);

    public void Add(TopologyName name)
    {
        if (!_namesSet.Add(name))
        {
            throw new InvalidOperationException(
                $"{ToSentenceCase(Direction)} topology '{name.Value}' is already registered. Registered {Direction} topologies: {FormatNames(_names)}."
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

    protected static string ToSentenceCase(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(value));
        }

        return char.ToUpperInvariant(value[0]) + value.Substring(1);
    }
}
