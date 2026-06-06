using System;

namespace Usf.Core.Messaging.Errors;

public sealed class TopologyEntryNotFoundException : Exception
{
    public TopologyEntryNotFoundException(string name)
        : base($"Topology entry '{name}' is not registered.")
    {
        Name = name;
    }

    public string Name { get; }
}
