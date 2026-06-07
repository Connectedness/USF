using System;
using FluentAssertions;
using Usf.Core.Messaging.Errors;
using Xunit;

namespace Usf.Core.Tests.Messaging.Errors;

public sealed class TopologyValidationExceptionTests
{
    [Fact]
    public void ValidationErrors_AreSortedDeterministically()
    {
        TopologyValidationException exception = new (["zeta", "alpha", "beta"]);

        exception.ValidationErrors.Should().Equal("alpha", "beta", "zeta");
    }

    [Fact]
    public void Constructor_RequiresAtLeastOneError()
    {
        Action action = () => _ = new TopologyValidationException(Array.Empty<string>());

        action.Should().Throw<ArgumentException>();
    }
}
