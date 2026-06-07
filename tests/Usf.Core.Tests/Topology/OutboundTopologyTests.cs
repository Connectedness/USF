using System;
using System.Collections.Generic;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Topology;

public sealed class OutboundTopologyTests
{
    [Fact]
    public void GetRequiredTarget_SupportsNamedTargetLookup()
    {
        var target = new RecordingTarget<SampleMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new Core.Messaging.Topology(
            TopologyName.Default,
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["named"] = target
            },
            new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
        );

        var resolvedTarget = topology.GetRequiredTarget("named");
        var typedTarget = topology.GetRequiredTarget<SampleMessage>("named");

        resolvedTarget.Should().BeSameAs(target);
        typedTarget.Should().BeSameAs(target);
    }
}
