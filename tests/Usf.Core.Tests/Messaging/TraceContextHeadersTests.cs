using System.Collections.Generic;
using System.Diagnostics;
using FluentAssertions;
using Usf.Core.Messaging;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class TraceContextHeadersTests
{
    [Fact]
    public void Inject_InjectsCurrentActivityIntoStringHeaders()
    {
        using var activity = new Activity("string-headers").SetIdFormat(ActivityIdFormat.W3C);
        activity.TraceStateString = "vendor=value";
        activity.AddBaggage("tenant", "tenant-7").Start();
        var headers = new Dictionary<string, string?>();

        TraceContextHeaders.Inject(headers);

        headers.Should().ContainKey("traceparent").WhoseValue.Should().Be(activity.Id);
        headers.Should().ContainKey("tracestate").WhoseValue.Should().Be("vendor=value");
        headers.Should().ContainKey("baggage");
        headers["baggage"]!.Replace(" ", string.Empty).Should().Be("tenant=tenant-7");
    }

    [Fact]
    public void Inject_InjectsProvidedActivityIntoObjectHeaders()
    {
        using var activity = new Activity("object-headers")
           .SetIdFormat(ActivityIdFormat.W3C)
           .Start();
        var headers = new Dictionary<string, object?>();

        TraceContextHeaders.Inject(headers, activity);

        headers.Should().ContainKey("traceparent").WhoseValue.Should().Be(activity.Id);
    }

    [Fact]
    public void Inject_DoesNothingWhenNoActivityIsCurrent()
    {
        var previousActivity = Activity.Current;
        Activity.Current = null;

        try
        {
            var stringHeaders = new Dictionary<string, string?>
            {
                ["tenant"] = "tenant-7"
            };
            var objectHeaders = new Dictionary<string, object?>
            {
                ["tenant"] = "tenant-7"
            };

            TraceContextHeaders.Inject(stringHeaders);
            TraceContextHeaders.Inject(objectHeaders);

            stringHeaders.Should().ContainSingle().Which.Should()
               .Be(new KeyValuePair<string, string?>("tenant", "tenant-7"));
            objectHeaders.Should().ContainSingle().Which.Should()
               .Be(new KeyValuePair<string, object?>("tenant", "tenant-7"));
        }
        finally
        {
            Activity.Current = previousActivity;
        }
    }

    [Fact]
    public void Inject_DoesNothingForHierarchicalActivities()
    {
        using var activity = new Activity("hierarchical")
           .SetIdFormat(ActivityIdFormat.Hierarchical)
           .Start();
        var headers = new Dictionary<string, string?>();

        TraceContextHeaders.Inject(headers);

        headers.Should().BeEmpty();
    }
}
