using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using FluentAssertions;
using Usf.Core.Messaging.Serialization;
using Xunit;

namespace Usf.Core.Tests.Messaging.Serialization;

public sealed class Utf8JsonPayloadCodecTests
{
    [Fact]
    public void Encode_UsesDefaultSerializerOptions_WhenConstructedWithoutOptions()
    {
        var codec = new Utf8JsonPayloadCodec();

        var encodedPayload = codec.Encode(
            new SerializerExactMessage("Hello", 7)
        );

        GetJson(encodedPayload).Should().Be("{\"Text\":\"Hello\",\"Count\":7}");
        encodedPayload.DataContentType.Should().Be("application/json");
    }

    [Fact]
    public void Encode_UsesConfiguredOptions_WhenSerializing()
    {
        var codec = new Utf8JsonPayloadCodec(Utf8JsonPayloadCodecCamelCaseContext.Default.Options);

        var encodedPayload = codec.Encode(
            new SerializerExactMessage("Hello", 7)
        );

        GetJson(encodedPayload).Should().Be("{\"text\":\"Hello\",\"count\":7}");
    }

    [Fact]
    public void Encode_UsesDeclaredTypeMetadata_WhenDeclaredTypeMatchesRuntimeType()
    {
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonPayloadCodecContext.Default
        };
        var codec = new Utf8JsonPayloadCodec(options);

        var encodedPayload = codec.Encode(
            new SerializerExactMessage("Hello", 7)
        );

        GetJson(encodedPayload).Should().Be("{\"Text\":\"Hello\",\"Count\":7}");
    }

    [Fact]
    public void Encode_UsesRuntimeTypeMetadata_WhenDeclaredTypeMetadataDoesNotCoverTheRuntimeType()
    {
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonPayloadCodecContext.Default
        };
        var codec = new Utf8JsonPayloadCodec(options);
        SerializerBaseMessage message = new SerializerDerivedMessage("Base", "Derived");

        var encodedPayload = codec.Encode(message);

        GetJson(encodedPayload).Should().Be("{\"DerivedValue\":\"Derived\",\"BaseValue\":\"Base\"}");
    }

    [Fact]
    public void Encode_KeepsDeclaredTypeMetadata_WhenItAlreadyCoversTheRuntimeType()
    {
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonPayloadCodecPolymorphicContext.Default
        };
        var codec = new Utf8JsonPayloadCodec(options);
        SerializerPolymorphicBaseMessage message = new SerializerKnownDerivedMessage("Base", "Derived");

        var encodedPayload = codec.Encode(message);

        GetJson(encodedPayload)
           .Should()
           .Be("{\"$kind\":\"known\",\"KnownValue\":\"Derived\",\"BaseValue\":\"Base\"}");
    }

    [Fact]
    public void Encode_FallsBackToNearestAncestor_WhenPolymorphismOptionsAllowIt()
    {
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonPayloadCodecFallbackContext.Default
        };
        var codec = new Utf8JsonPayloadCodec(options);
        SerializerFallbackBaseMessage message = new SerializerFallbackLeafMessage("Base", "Known", "Leaf");

        var encodedPayload = codec.Encode(message);

        GetJson(encodedPayload)
           .Should()
           .Be("{\"$kind\":\"known\",\"KnownValue\":\"Known\",\"BaseValue\":\"Base\"}");
    }

    [Fact]
    public void Encode_ThrowsDeterministicException_WhenRuntimeMetadataIsMissing()
    {
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonPayloadCodecMissingRuntimeContext.Default
        };
        var codec = new Utf8JsonPayloadCodec(options);
        SerializerBaseMessage message = new SerializerDerivedMessage("Base", "Derived");

        Action action = () => codec.Encode(message);

        action.Should()
           .Throw<InvalidOperationException>()
           .WithMessage(
                "*JsonTypeInfo metadata for type 'Usf.Core.Tests.Messaging.Serialization.SerializerDerivedMessage'*"
            );
    }

    [Fact]
    public void Encode_RejectsNullMessages()
    {
        var codec = new Utf8JsonPayloadCodec();

        Action action = () => codec.Encode<string>(null!);

        action.Should().Throw<ArgumentNullException>().WithParameterName("message");
    }

    [Fact]
    public void Decode_DeserializesFromReadOnlyMemorySlice()
    {
        var codec = new Utf8JsonPayloadCodec(Utf8JsonPayloadCodecContext.Default.Options);
        var paddedJson = "x{\"Text\":\"Hello\",\"Count\":7}y"u8.ToArray();
        ReadOnlyMemory<byte> json = paddedJson.AsMemory(1, paddedJson.Length - 2);

        var message = codec.Decode(json, typeof(SerializerExactMessage));

        message.Should().Be(new SerializerExactMessage("Hello", 7));
    }

    private static string GetJson(EncodedPayload encodedPayload)
    {
        return Encoding.UTF8.GetString(encodedPayload.Data);
    }
}

public sealed record SerializerExactMessage(string Text, int Count);

public record SerializerBaseMessage(string BaseValue);

public sealed record SerializerDerivedMessage(string BaseValue, string DerivedValue) : SerializerBaseMessage(BaseValue);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$kind")]
[JsonDerivedType(typeof(SerializerKnownDerivedMessage), "known")]
public record SerializerPolymorphicBaseMessage(string BaseValue);

public sealed record SerializerKnownDerivedMessage(string BaseValue, string KnownValue) :
    SerializerPolymorphicBaseMessage(BaseValue);

[JsonPolymorphic(
    TypeDiscriminatorPropertyName = "$kind",
    UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToNearestAncestor
)]
[JsonDerivedType(typeof(SerializerFallbackKnownDerivedMessage), "known")]
public record SerializerFallbackBaseMessage(string BaseValue);

public record SerializerFallbackKnownDerivedMessage(string BaseValue, string KnownValue) :
    SerializerFallbackBaseMessage(BaseValue);

public sealed record SerializerFallbackLeafMessage(string BaseValue, string KnownValue, string LeafValue) :
    SerializerFallbackKnownDerivedMessage(BaseValue, KnownValue);

[JsonSerializable(typeof(SerializerExactMessage))]
[JsonSerializable(typeof(SerializerBaseMessage))]
[JsonSerializable(typeof(SerializerDerivedMessage))]
public partial class Utf8JsonPayloadCodecContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(SerializerExactMessage))]
public partial class Utf8JsonPayloadCodecCamelCaseContext : JsonSerializerContext;

[JsonSerializable(typeof(SerializerPolymorphicBaseMessage))]
[JsonSerializable(typeof(SerializerKnownDerivedMessage))]
public partial class Utf8JsonPayloadCodecPolymorphicContext : JsonSerializerContext;

[JsonSerializable(typeof(SerializerFallbackBaseMessage))]
[JsonSerializable(typeof(SerializerFallbackKnownDerivedMessage))]
public partial class Utf8JsonPayloadCodecFallbackContext : JsonSerializerContext;

[JsonSerializable(typeof(SerializerBaseMessage))]
public partial class Utf8JsonPayloadCodecMissingRuntimeContext : JsonSerializerContext;
