using System;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Abstractions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Xunit;

namespace Usf.Core.Tests.Messaging.Serialization;

public sealed class CloudEventMessageSerializerTests
{
    [Fact]
    public async Task SerializeAsync_AssemblesEnvelopeFromMetadataOptionsRegistryAndCodec()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var serializer = new CloudEventMessageSerializer(
            new StubPayloadCodec(new EncodedPayload("body"u8.ToArray(), "application/custom")),
            new CloudEventsOptions
            {
                Source = "/configured"
            }
        );
        var id = Guid.Parse("AB150CD4-692C-4C0F-AD47-A187957860F4");
        var time = new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero);
        CloudEventMetadata metadata = new (id, time, "subject-7", "/override");

        var envelope = await serializer.SerializeAsync(
            new EnvelopeMessage("hello"),
            in metadata,
            "tests.envelope",
            "/schemas/envelope",
            cancellationToken
        );

        var expectedEnvelope = new CloudEventEnvelope(
            "1.0",
            "ab150cd4-692c-4c0f-ad47-a187957860f4",
            "/override",
            "tests.envelope",
            time,
            "subject-7",
            "application/custom",
            "/schemas/envelope",
            "body"u8.ToArray()
        );
        envelope.Should().Be(expectedEnvelope);
    }

    [Fact]
    public async Task SerializeAsync_UsesProvidedType_WithoutConsultingTheRegistry()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var serializer = new CloudEventMessageSerializer(
            new StubPayloadCodec(new EncodedPayload("body"u8.ToArray(), "application/custom")),
            new CloudEventsOptions
            {
                Source = "/configured"
            }
        );
        CloudEventMetadata metadata = new (
            Guid.Parse("AB150CD4-692C-4C0F-AD47-A187957860F4"),
            new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero)
        );

        var envelope = await serializer.SerializeAsync(
            new EnvelopeMessage("hello"),
            in metadata,
            "already.resolved",
            dataSchema: null,
            cancellationToken
        );

        envelope.Type.Should().Be("already.resolved");
    }

    [Theory]
    [MemberData(nameof(GetMissingMetadataCases))]
    public async Task SerializeAsync_RejectsMissingRequiredMetadata(
        CloudEventMetadata metadata,
        CloudEventsOptions options,
        string? type,
        string expectedAttribute
    )
    {
        var serializer = new CloudEventMessageSerializer(
            new StubPayloadCodec(new EncodedPayload("body"u8.ToArray(), "application/custom")),
            options
        );

        var action = async () => await serializer.SerializeAsync(
            new EnvelopeMessage("hello"),
            in metadata,
            type,
            dataSchema: null
        );

        var exception = (await action.Should().ThrowAsync<CloudEventMetadataException>()).Which;
        exception.AttributeName.Should().Be(expectedAttribute);
    }

    public static TheoryData<CloudEventMetadata, CloudEventsOptions, string?, string>
        GetMissingMetadataCases()
    {
        var id = Guid.Parse("cb2fcd1b-a98e-43ab-b634-7a40174b3fd6");
        var time = new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero);

        return new TheoryData<CloudEventMetadata, CloudEventsOptions, string?, string>
        {
            {
                new CloudEventMetadata(Guid.Empty, time),
                new CloudEventsOptions { Source = "/source" },
                "tests.envelope",
                CloudEventAttributeNames.Id
            },
            {
                new CloudEventMetadata(id, default),
                new CloudEventsOptions { Source = "/source" },
                "tests.envelope",
                CloudEventAttributeNames.Time
            },
            {
                new CloudEventMetadata(id, time),
                new CloudEventsOptions(),
                "tests.envelope",
                CloudEventAttributeNames.Source
            },
            {
                new CloudEventMetadata(id, time),
                new CloudEventsOptions { Source = "/source" },
                null,
                CloudEventAttributeNames.Type
            }
        };
    }

    private sealed record EnvelopeMessage(string Value);

    private sealed class StubPayloadCodec : IPayloadCodec
    {
        private readonly EncodedPayload _payload;

        public StubPayloadCodec(EncodedPayload payload)
        {
            _payload = payload;
        }

        public EncodedPayload Encode<T>(T message)
        {
            return _payload;
        }

        public object? Decode(byte[] data, Type messageType)
        {
            return new EnvelopeMessage("decoded");
        }
    }
}
