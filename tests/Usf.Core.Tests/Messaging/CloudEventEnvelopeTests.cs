using System;
using System.Collections.Generic;
using FluentAssertions;
using Usf.Core.Messaging;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class CloudEventEnvelopeTests
{
    [Fact]
    public void Equals_ComparesDataByItem()
    {
        var left = CreateEnvelope(data: new byte[] { 1, 2, 3 });
        var right = CreateEnvelope(data: new byte[] { 1, 2, 3 });

        left.Should().Be(right);
    }

    [Fact]
    public void Equals_ComparesExtensionsByItem()
    {
        var left = CreateEnvelope(
            extensions: new Dictionary<string, string?>
            {
                ["alpha"] = "one",
                ["beta"] = null
            }
        );
        var right = CreateEnvelope(
            extensions: new Dictionary<string, string?>
            {
                ["beta"] = null,
                ["alpha"] = "one"
            }
        );

        left.Should().Be(right);
    }

    [Fact]
    public void Equals_DetectsItemDifferences()
    {
        var left = CreateEnvelope(
            data: new byte[] { 1, 2, 3 },
            extensions: new Dictionary<string, string?> { ["alpha"] = "one" }
        );
        var right = CreateEnvelope(
            data: new byte[] { 1, 2, 4 },
            extensions: new Dictionary<string, string?> { ["alpha"] = "two" }
        );

        left.Should().NotBe(right);
    }

    [Fact]
    public void GetHashCode_CreatesSameHashCodeForEqualEnvelopes()
    {
        var left = CreateEnvelope(
            data: new byte[] { 5, 2, 8, 42, 9 },
            extensions: new Dictionary<string, string?> { ["foo"] = "bar", ["baz"] = "quz" }
        );
        var right = CreateEnvelope(
            data: new byte[] { 5, 2, 8, 42, 9 },
            extensions: new Dictionary<string, string?> { ["foo"] = "bar", ["baz"] = "quz" }
        );

        left.GetHashCode().Should().Be(right.GetHashCode());
    }

    private static CloudEventEnvelope CreateEnvelope(
        ReadOnlyMemory<byte>? data = null,
        IReadOnlyDictionary<string, string?>? extensions = null
    )
    {
        return new CloudEventEnvelope(
            "1.0",
            "ab150cd4-692c-4c0f-ad47-a187957860f4",
            "/source",
            "tests.envelope",
            new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero),
            "subject-7",
            "application/octet-stream",
            "/schemas/envelope",
            data ?? new ReadOnlyMemory<byte>([1, 2, 3]),
            extensions
        );
    }
}
