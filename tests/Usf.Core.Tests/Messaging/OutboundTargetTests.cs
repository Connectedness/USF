using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class OutboundTargetTests
{
    [Fact]
    public async Task PublishAsync_ForwardsRoutingKeyToTypedDispatch()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>(
            "default",
            CloudEventsTestFactory.CreateSerializer()
        );

        await target.PublishAsync(
            new SampleMessage("hello"),
            "target.route",
            cancellationToken
        );

        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("target.route");
    }

    [Fact]
    public async Task PublishAsync_WrapsSerializationFailures()
    {
        var serializer = new ThrowingSerializer(new InvalidOperationException("boom"));
        var target = new RecordingTarget<SampleMessage>("default", serializer);

        var action = async () => await target.PublishAsync(new SampleMessage("hello"));

        var exception = (await action.Should().ThrowAsync<MessageSerializationException>()).Which;
        exception.InnerException.Should().BeOfType<InvalidOperationException>();
        exception.MessageType.Should().Be<SampleMessage>();
    }

    [Fact]
    public async Task PublishAsync_DoesNotWrapCancellation()
    {
        var serializer = new ThrowingSerializer(new OperationCanceledException());
        var target = new RecordingTarget<SampleMessage>("default", serializer);

        var action = async () => await target.PublishAsync(new SampleMessage("hello"));

        await action.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task PublishAsync_ReportsMissingCloudEventIdWhenMessageDoesNotProvideMetadata()
    {
        var target = new RecordingTarget<ThirdPartyMessage>(
            "third-party",
            CloudEventsTestFactory.CreateSerializer(),
            CloudEventsTestFactory.CreateRegistry(
                new KeyValuePair<Type, string>(typeof(ThirdPartyMessage), "tests.third-party")
            )
        );

        var action = async () => await target.PublishAsync(new ThirdPartyMessage("hello"));

        var exception = (await action.Should().ThrowAsync<CloudEventMetadataException>()).Which;
        exception.AttributeName.Should().Be(CloudEventAttributeNames.Id);
    }
}
