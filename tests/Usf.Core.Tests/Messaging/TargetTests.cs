using System;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging.Errors;
using Usf.Core.Tests.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class TargetTests
{
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
}
