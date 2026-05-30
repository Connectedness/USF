using System;
using FluentAssertions;
using Usf.Core.Messaging.Errors;
using Xunit;

namespace Usf.Core.Tests.Messaging.Errors;

public sealed class MessageDeliveryExceptionTests
{
    [Fact]
    public void Constructor_RejectsBlankTargetName()
    {
        var action = () => _ = new MessageDeliveryException(
            " ",
            MessageDeliveryFailureReason.Timeout
        );

        action.Should().Throw<ArgumentException>().WithParameterName("targetName");
    }

    [Fact]
    public void Constructor_RejectsInnerExceptionForTimeout()
    {
        var action = () => _ = new MessageDeliveryException(
            "target",
            MessageDeliveryFailureReason.Timeout,
            new InvalidOperationException()
        );

        action.Should().Throw<ArgumentException>()
           .WithParameterName("innerException")
           .WithMessage("A delivery timeout cannot provide an inner exception.*");
    }

    [Theory]
    [InlineData(MessageDeliveryFailureReason.Nacked)]
    [InlineData(MessageDeliveryFailureReason.Returned)]
    public void Constructor_RequiresInnerExceptionForBrokerFailure(MessageDeliveryFailureReason reason)
    {
        var action = () => _ = new MessageDeliveryException("target", reason);

        action.Should().Throw<ArgumentException>()
           .WithParameterName("innerException")
           .WithMessage("A delivery failure other than timeout must provide an inner exception.*");
    }
}
