using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

/// <summary>
/// Builds an empty <see cref="Topology" /> for tests that need a topology with no outbound targets and
/// no inbound endpoints.
/// </summary>
public static class EmptyTopology
{
    public static Topology Create()
    {
        return new TestTopology(Topology.DefaultName);
    }
}
