using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class MessagePublisherTests
{
    [Fact]
    public async Task PublishMessageAsync_UsesTopologyResolvedTarget_WhenNoExplicitTargetIsProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>("default", new Utf8JsonMessageSerializer());
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
        var serializedMessage = target.SerializedMessages.Should().ContainSingle().Which;
        Encoding.UTF8.GetString(serializedMessage.Body).Should().Be("{\"Value\":\"hello\"}");
        serializedMessage.ContentType.Should().Be("application/json");
        serializedMessage.ContentEncoding.Should().Be("utf-8");
        serializedMessage.Headers.Should().BeEmpty();
        serializedMessage.MessageId.Should().BeNull();
        serializedMessage.CorrelationId.Should().BeNull();
    }

    [Fact]
    public async Task PublishMessageAsync_UsesExplicitTarget_WhenProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var explicitTarget = new RecordingTarget<SampleMessage>("explicit", new Utf8JsonMessageSerializer());
        var publisher = new MessagePublisher(new EmptyMessageTopology());
        var message = new SampleMessage("hello");

        await publisher.PublishMessageAsync(message, explicitTarget, cancellationToken);

        explicitTarget.Messages.Should().ContainSingle().Which.Should().Be(message);
        Encoding.UTF8.GetString(explicitTarget.SerializedMessages.Should().ContainSingle().Which.Body)
           .Should()
           .Be("{\"Value\":\"hello\"}");
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
        var target = new RecordingTarget<OtherMessage>("other", new Utf8JsonMessageSerializer());
        var publisher = new MessagePublisher(new EmptyMessageTopology());

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"), target);

        await action.Should().ThrowAsync<MessageTargetTypeMismatchException>();
    }
}
