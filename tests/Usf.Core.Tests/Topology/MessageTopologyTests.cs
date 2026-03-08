using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Tests.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Topology;

public sealed class MessageTopologyTests
{
    [Fact]
    public void GetRequiredTarget_SupportsNamedTargetLookup()
    {
        var serializer = new RecordingSerializer(
            new SerializedMessage(
                [5],
                null,
                null,
                new ReadOnlyDictionary<string, string?>(new Dictionary<string, string?>()),
                null,
                null
            )
        );
        var target = new RecordingTarget<SampleMessage>("named", serializer);
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
