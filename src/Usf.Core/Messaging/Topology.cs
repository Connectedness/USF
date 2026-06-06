using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public abstract class Topology<TEntry>
{
    private readonly IReadOnlyDictionary<string, TEntry> _entriesByName;

    protected Topology(IDictionary<string, TEntry> entriesByName)
    {
        if (entriesByName is null)
        {
            throw new ArgumentNullException(nameof(entriesByName));
        }

        _entriesByName = new ReadOnlyDictionary<string, TEntry>(
            new Dictionary<string, TEntry>(entriesByName, StringComparer.Ordinal)
        );
        Entries = _entriesByName.Values.ToArray();
    }

    public IReadOnlyCollection<TEntry> Entries { get; }

    public TEntry GetRequired(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_entriesByName.TryGetValue(name, out var entry))
        {
            throw new TopologyEntryNotFoundException(name);
        }

        return entry;
    }

    public bool TryGet(string name, out TEntry? entry)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        return _entriesByName.TryGetValue(name, out entry);
    }

    protected bool TryGetEntry(string name, out TEntry? entry)
    {
        return _entriesByName.TryGetValue(name, out entry);
    }
}
