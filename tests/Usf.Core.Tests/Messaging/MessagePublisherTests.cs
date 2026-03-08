using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Tests.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class MessagePublisherTests
{
    [Fact]
    public async Task PublishMessageAsync_UsesTopologyResolvedTarget_WhenNoExplicitTargetIsProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var serializer = new RecordingSerializer(
            new SerializedMessage(
                [1, 2, 3],
                "application/octet-stream",
                null,
                new ReadOnlyDictionary<string, string?>(new Dictionary<string, string?>()),
                null,
                null
            )
        );
        var target = new RecordingTarget<SampleMessage>("default", serializer);
        var topology = new MessageTopology(
            new Dictionary<Type, Target>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, Target>(StringComparer.Ordinal)
        );
        var publisher = new MessagePublisher(topology);
        var message = new SampleMessage("hello");

        await publisher.PublishMessageAsync(message, cancellationToken: cancellationToken);

        target.Messages.Should().ContainSingle().Which.Should().Be(message);
        serializer.Messages.Should().ContainSingle().Which.Should().Be(message);
    }

    [Fact]
    public async Task PublishMessageAsync_UsesExplicitTarget_WhenProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var serializer = new RecordingSerializer(
            new SerializedMessage(
                [9],
                "application/octet-stream",
                null,
                new ReadOnlyDictionary<string, string?>(new Dictionary<string, string?>()),
                null,
                null
            )
        );
        var explicitTarget = new RecordingTarget<SampleMessage>("explicit", serializer);
        var publisher = new MessagePublisher(new EmptyMessageTopology());
        var message = new SampleMessage("hello");

        await publisher.PublishMessageAsync(message, explicitTarget, cancellationToken);

        explicitTarget.Messages.Should().ContainSingle().Which.Should().Be(message);
    }

    [Fact]
    public async Task PublishMessageAsync_RejectsNullMessages()
    {
        var publisher = new MessagePublisher(new EmptyMessageTopology());

        var action = async () => await publisher.PublishMessageAsync<string>(null!);

        await action.Should().ThrowAsync<ArgumentNullException>().WithParameterName("message");
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsWhenNoTargetIsConfigured()
    {
        var publisher = new MessagePublisher(new EmptyMessageTopology());

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"));

        await action.Should().ThrowAsync<MessageTargetNotFoundException>();
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsWhenExplicitTargetDoesNotMatchMessageType()
    {
        var serializer = new RecordingSerializer(
            new SerializedMessage(
                [4],
                null,
                null,
                new ReadOnlyDictionary<string, string?>(new Dictionary<string, string?>()),
                null,
                null
            )
        );
        var target = new RecordingTarget<OtherMessage>("other", serializer);
        var publisher = new MessagePublisher(new EmptyMessageTopology());

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"), target);

        await action.Should().ThrowAsync<MessageTargetTypeMismatchException>();
    }
}
