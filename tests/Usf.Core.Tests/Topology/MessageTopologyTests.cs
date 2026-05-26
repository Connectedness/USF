using System;
using System.Collections.Generic;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Topology;

public sealed class MessageTopologyTests
{
    [Fact]
    public void GetRequiredTarget_SupportsNamedTargetLookup()
    {
        var target = new RecordingTarget<SampleMessage>("named", new Utf8JsonMessageSerializer());
        var topology = new MessageTopology(
            new Dictionary<Type, Target>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, Target>(StringComparer.Ordinal)
            {
                ["named"] = target
            }
        );

        var resolvedTarget = topology.GetRequiredTarget("named");
        var typedTarget = topology.GetRequiredTarget<SampleMessage>("named");

        resolvedTarget.Should().BeSameAs(target);
        typedTarget.Should().BeSameAs(target);
    }
}
