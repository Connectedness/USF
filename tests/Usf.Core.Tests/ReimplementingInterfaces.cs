using System;
using FluentAssertions;
using Xunit;

namespace Usf.Core.Tests;

public class ReimplementingInterfaces
{
    [Fact]
    public void TestMethodName()
    {
        IDisposable actuallyBar = new Bar();

        var act = () => actuallyBar.Dispose();

        act.Should().Throw<NotImplementedException>().Where(x => x.Message == "Bar");
    }
}

public class Foo : IDisposable
{
    public void Dispose() => throw new NotImplementedException("Foo");
}

public class Bar : Foo, IDisposable
{
    public new void Dispose() => throw new NotImplementedException("Bar");
}
