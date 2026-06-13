using System;
using System.Collections.Generic;
using FluentAssertions;
using Usf.Core.Messaging;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class TransportMessageTests
{
    [Fact]
    public void Constructor_StoresBodyAndHeadersWithoutCopying()
    {
        var bodyBytes = new byte[] { 1, 2, 3 };
        ReadOnlyMemory<byte> body = bodyBytes.AsMemory(1, 2);
        var headers = new Dictionary<string, object?> { ["name"] = "original" };

        var message = new TestTransportMessage(body, headers);
        bodyBytes[1] = 9;
        headers["name"] = "changed";

        message.Body.Span.ToArray().Should().Equal(9, 3);
        message.Headers.Should().BeSameAs(headers);
        message.Headers["name"].Should().Be("changed");
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage(
            ReadOnlyMemory<byte> body,
            IReadOnlyDictionary<string, object?> headers
        )
            : base("test", "source", body, headers) { }
    }
}
