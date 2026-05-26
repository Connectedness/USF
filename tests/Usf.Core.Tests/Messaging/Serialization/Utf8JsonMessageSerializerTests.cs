using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;
using Xunit;

namespace Usf.Core.Tests.Messaging.Serialization;

public sealed class Utf8JsonMessageSerializerTests
{
    [Fact]
    public async Task SerializeAsync_UsesDefaultSerializerOptions_WhenConstructedWithoutOptions()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var serializer = new Utf8JsonMessageSerializer();

        var serializedMessage = await serializer.SerializeAsync(
            new SerializerExactMessage("Hello", 7),
            cancellationToken
        );

        GetJson(serializedMessage).Should().Be("{\"Text\":\"Hello\",\"Count\":7}");
        serializedMessage.ContentType.Should().Be("application/json");
        serializedMessage.ContentEncoding.Should().Be("utf-8");
        serializedMessage.Headers.Should().BeEmpty();
        serializedMessage.MessageId.Should().BeNull();
        serializedMessage.CorrelationId.Should().BeNull();
    }

    [Fact]
    public async Task SerializeAsync_UsesConfiguredOptions_WhenSerializing()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var serializer = new Utf8JsonMessageSerializer(Utf8JsonMessageSerializerCamelCaseContext.Default.Options);

        var serializedMessage = await serializer.SerializeAsync(
            new SerializerExactMessage("Hello", 7),
            cancellationToken
        );

        GetJson(serializedMessage).Should().Be("{\"text\":\"Hello\",\"count\":7}");
    }

    [Fact]
    public async Task SerializeAsync_UsesDeclaredTypeMetadata_WhenDeclaredTypeMatchesRuntimeType()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonMessageSerializerContext.Default
        };
        var serializer = new Utf8JsonMessageSerializer(options);

        var serializedMessage = await serializer.SerializeAsync(
            new SerializerExactMessage("Hello", 7),
            cancellationToken
        );

        GetJson(serializedMessage).Should().Be("{\"Text\":\"Hello\",\"Count\":7}");
    }

    [Fact]
    public async Task SerializeAsync_UsesRuntimeTypeMetadata_WhenDeclaredTypeMetadataDoesNotCoverTheRuntimeType()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonMessageSerializerContext.Default
        };
        var serializer = new Utf8JsonMessageSerializer(options);
        SerializerBaseMessage message = new SerializerDerivedMessage("Base", "Derived");

        var serializedMessage = await serializer.SerializeAsync(message, cancellationToken);
        var json = GetJson(serializedMessage);

        json.Should().Contain("\"BaseValue\":\"Base\"");
        json.Should().Contain("\"DerivedValue\":\"Derived\"");
    }

    [Fact]
    public async Task SerializeAsync_KeepsDeclaredTypeMetadata_WhenItAlreadyCoversTheRuntimeType()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonMessageSerializerPolymorphicContext.Default
        };
        var serializer = new Utf8JsonMessageSerializer(options);
        SerializerPolymorphicBaseMessage message = new SerializerKnownDerivedMessage("Base", "Derived");

        var serializedMessage = await serializer.SerializeAsync(message, cancellationToken);
        var json = GetJson(serializedMessage);

        json.Should().Contain("\"$kind\":\"known\"");
        json.Should().Contain("\"BaseValue\":\"Base\"");
        json.Should().Contain("\"KnownValue\":\"Derived\"");
    }

    [Fact]
    public async Task SerializeAsync_ThrowsDeterministicException_WhenRuntimeMetadataIsMissing()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = Utf8JsonMessageSerializerMissingRuntimeContext.Default
        };
        var serializer = new Utf8JsonMessageSerializer(options);
        SerializerBaseMessage message = new SerializerDerivedMessage("Base", "Derived");

        var action = async () => await serializer.SerializeAsync(message, cancellationToken);

        await action.Should()
           .ThrowAsync<InvalidOperationException>()
           .WithMessage(
                "*JsonTypeInfo metadata for type 'Usf.Core.Tests.Messaging.Serialization.SerializerDerivedMessage'*"
            );
    }

    [Fact]
    public async Task SerializeAsync_RejectsNullMessages()
    {
        var serializer = new Utf8JsonMessageSerializer();

        var action = async () => await serializer.SerializeAsync<string>(null!);

        await action.Should().ThrowAsync<ArgumentNullException>().WithParameterName("message");
    }

    private static string GetJson(SerializedMessage serializedMessage)
    {
        return Encoding.UTF8.GetString(serializedMessage.Body);
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

[JsonSerializable(typeof(SerializerExactMessage))]
[JsonSerializable(typeof(SerializerBaseMessage))]
[JsonSerializable(typeof(SerializerDerivedMessage))]
public partial class Utf8JsonMessageSerializerContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(SerializerExactMessage))]
public partial class Utf8JsonMessageSerializerCamelCaseContext : JsonSerializerContext;

[JsonSerializable(typeof(SerializerPolymorphicBaseMessage))]
[JsonSerializable(typeof(SerializerKnownDerivedMessage))]
public partial class Utf8JsonMessageSerializerPolymorphicContext : JsonSerializerContext;

[JsonSerializable(typeof(SerializerBaseMessage))]
public partial class Utf8JsonMessageSerializerMissingRuntimeContext : JsonSerializerContext;
