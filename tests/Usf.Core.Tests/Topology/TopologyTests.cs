using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Topologies;

public sealed class TopologyTests
{
    [Fact]
    public void GetRequiredTarget_SupportsNamedTargetLookup()
    {
        var target = new RecordingTarget<SampleMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
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

    [Fact]
    public void Constructor_RejectsInvalidName()
    {
        var action = () => new TestTopology("   ");

        action.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Fact]
    public void Lookups_UseOrdinalNameComparison()
    {
        var target = new RecordingTarget<SampleMessage>("Named", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
            targetsByName: new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["Named"] = target
            }
        );

        var action = () => topology.GetRequiredTarget("named");

        action.Should().Throw<OutboundTargetNotFoundException>();
    }

    [Fact]
    public void StateProperties_DescribeEmptyTopology()
    {
        var topology = new TestTopology(Topology.DefaultName);

        topology.IsEmpty.Should().BeTrue();
        topology.IsOutboundOnly.Should().BeFalse();
        topology.IsInboundOnly.Should().BeFalse();
    }

    [Fact]
    public void StateProperties_DescribeOutboundOnlyTopology()
    {
        var target = new RecordingTarget<SampleMessage>("target", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
            targetsByMessageType: new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            }
        );

        topology.IsEmpty.Should().BeFalse();
        topology.IsOutboundOnly.Should().BeTrue();
        topology.IsInboundOnly.Should().BeFalse();
    }

    [Fact]
    public void StateProperties_DescribeInboundOnlyTopology()
    {
        var endpoint = new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(SampleMessageHandler),
            typeof(CloudEventMessageSerializer),
            "sample"
        );
        var topology = new TestTopology(
            Topology.DefaultName,
            endpointsByName: new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
            {
                [endpoint.Name] = endpoint
            }
        );

        topology.IsEmpty.Should().BeFalse();
        topology.IsOutboundOnly.Should().BeFalse();
        topology.IsInboundOnly.Should().BeTrue();
    }

    [Fact]
    public void RegistrationCatalog_RejectsInvalidAndDuplicateNamesWithOrdinalComparison()
    {
        TopologyRegistrationCatalog catalog = new ();
        catalog.Add("orders");
        catalog.Add("ORDERS");

        var invalidAction = () => catalog.Add(" ");
        var duplicateAction = () => catalog.Add("orders");

        invalidAction.Should().Throw<ArgumentException>().WithParameterName("name");
        duplicateAction.Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'orders' is already registered. Registered topologies: ORDERS, orders.");
    }

    private sealed class SampleMessageHandler : IMessageHandler<SampleMessage>
    {
        public Task HandleAsync(
            SampleMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
