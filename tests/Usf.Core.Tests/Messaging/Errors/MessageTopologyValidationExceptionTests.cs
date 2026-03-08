using System;
using FluentAssertions;
using Usf.Core.Messaging.Errors;
using Xunit;

namespace Usf.Core.Tests.Messaging.Errors;

public sealed class MessageTopologyValidationExceptionTests
{
    [Fact]
    public void ValidationErrors_AreSortedDeterministically()
    {
        MessageTopologyValidationException exception = new (["zeta", "alpha", "beta"]);

        exception.ValidationErrors.Should().Equal("alpha", "beta", "zeta");
    }

    [Fact]
    public void Constructor_RequiresAtLeastOneError()
    {
        Action action = () => _ = new MessageTopologyValidationException(Array.Empty<string>());

        action.Should().Throw<ArgumentException>();
    }
}
