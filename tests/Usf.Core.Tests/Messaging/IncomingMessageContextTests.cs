using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class IncomingMessageContextTests
{
    [Fact]
    public void Items_AreStoredWithStronglyTypedKeys()
    {
        var key = new MessageContextKey<int>("count");
        var context = CreateContext();

        context.TryGetItem(key, out var missing).Should().BeFalse();
        missing.Should().Be(0);

        context.SetItem(key, 42);

        context.TryGetItem(key, out var value).Should().BeTrue();
        value.Should().Be(42);
        context.GetRequiredItem(key).Should().Be(42);
        context.RemoveItem(key).Should().BeTrue();
        context.TryGetItem(key, out _).Should().BeFalse();
    }

    private static IncomingMessageContext CreateContext()
    {
        return new IncomingMessageContext(
            new TestTransportMessage(),
            new InboundEndpoint<TestMessage>(
                "endpoint",
                "test",
                Topology.DefaultName,
                typeof(TestHandler),
                typeof(CloudEventMessageSerializer),
                "tests.message",
                MessageHandlerInvocation.Create<TestMessage, TestHandler>()
            ),
            EmptyServiceProvider.Instance,
            new RecordingAcknowledgement(),
            default
        );
    }

    private sealed record TestMessage;

    private sealed class TestHandler : IMessageHandler<TestMessage>
    {
        public Task HandleAsync(
            TestMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage()
            : base(
                "test",
                "source",
                [],
                new Dictionary<string, object?>()
            ) { }
    }

    private sealed class RecordingAcknowledgement : IMessageAcknowledgement
    {
        public Task AckAsync(
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }

        public Task NackAsync(
            bool requeue,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        private EmptyServiceProvider() { }
        public static EmptyServiceProvider Instance { get; } = new ();

        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}
