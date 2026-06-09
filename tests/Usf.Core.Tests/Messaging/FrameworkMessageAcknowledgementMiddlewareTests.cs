using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class FrameworkMessageAcknowledgementMiddlewareTests
{
    [Fact]
    public async Task InvokeAsync_AcksSuccessfulAutoAckEndpoint()
    {
        var acknowledgement = new RecordingAcknowledgement();
        var context = CreateContext(
            acknowledgement,
            MessageAckMode.Auto,
            TestContext.Current.CancellationToken
        );
        FrameworkMessageAcknowledgementMiddleware middleware = new ();

        await middleware.InvokeAsync(context, static _ => Task.CompletedTask);

        acknowledgement.Actions.Should().Equal("ack");
    }

    [Fact]
    public async Task InvokeAsync_NacksWithoutRequeueWhenHandlerFails()
    {
        var acknowledgement = new RecordingAcknowledgement();
        var context = CreateContext(
            acknowledgement,
            MessageAckMode.Auto,
            TestContext.Current.CancellationToken
        );
        FrameworkMessageAcknowledgementMiddleware middleware = new ();

        var action = async () => await middleware.InvokeAsync(
            context,
            static _ => throw new InvalidOperationException("failure")
        );

        await action.Should().ThrowAsync<InvalidOperationException>();
        acknowledgement.Actions.Should().Equal("nack:false");
    }

    [Fact]
    public async Task InvokeAsync_NacksWithRequeueWhenShutdownCancellationIsRequested()
    {
        var acknowledgement = new RecordingAcknowledgement();
        using CancellationTokenSource cancellationTokenSource = new ();
        cancellationTokenSource.Cancel();
        var context = CreateContext(acknowledgement, MessageAckMode.Auto, cancellationTokenSource.Token);
        FrameworkMessageAcknowledgementMiddleware middleware = new ();

        var action = async () => await middleware.InvokeAsync(
            context,
            _ => throw new OperationCanceledException(cancellationTokenSource.Token)
        );

        await action.Should().ThrowAsync<OperationCanceledException>();
        acknowledgement.Actions.Should().Equal("nack:true");
    }

    [Fact]
    public async Task InvokeAsync_DoesNotSettleManualAckEndpoint()
    {
        var acknowledgement = new RecordingAcknowledgement();
        var context = CreateContext(
            acknowledgement,
            MessageAckMode.Manual,
            TestContext.Current.CancellationToken
        );
        FrameworkMessageAcknowledgementMiddleware middleware = new ();

        await middleware.InvokeAsync(context, static _ => Task.CompletedTask);

        acknowledgement.Actions.Should().BeEmpty();
    }

    private static IncomingMessageContext CreateContext(
        RecordingAcknowledgement acknowledgement,
        MessageAckMode ackMode,
        CancellationToken cancellationToken = default
    )
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
                ackMode
            ),
            EmptyServiceProvider.Instance,
            acknowledgement,
            cancellationToken
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
        public List<string> Actions { get; } = [];

        public Task AckAsync(CancellationToken cancellationToken = default)
        {
            Actions.Add("ack");
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
        {
            Actions.Add($"nack:{requeue.ToString().ToLowerInvariant()}");
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
