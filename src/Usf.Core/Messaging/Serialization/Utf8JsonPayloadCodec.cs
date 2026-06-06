using System;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Usf.Core.Messaging.Serialization;

public sealed class Utf8JsonPayloadCodec : IPayloadCodec
{
    private static readonly JsonSerializerOptions DefaultSerializerOptions = CreateDefaultSerializerOptions();

    private readonly JsonSerializerOptions _serializerOptions;

    public Utf8JsonPayloadCodec() : this(DefaultSerializerOptions) { }

    public Utf8JsonPayloadCodec(JsonSerializerOptions serializerOptions)
    {
        _serializerOptions = serializerOptions is null ?
            throw new ArgumentNullException(nameof(serializerOptions)) :
            new JsonSerializerOptions(serializerOptions);
    }

    public EncodedPayload Encode<T>(T message)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var declaredTypeInfo = GetRequiredTypeInfo(typeof(T));
        var runtimeType = message.GetType();

        var utf8Bytes = ShouldUseWith(declaredTypeInfo, runtimeType) ?
            JsonSerializer.SerializeToUtf8Bytes(message, (JsonTypeInfo<T>) declaredTypeInfo) :
            JsonSerializer.SerializeToUtf8Bytes(message, GetRequiredTypeInfo(runtimeType));

        return new EncodedPayload(utf8Bytes, "application/json");
    }

    public object? Decode(byte[] data, Type messageType)
    {
        if (data is null)
        {
            throw new ArgumentNullException(nameof(data));
        }

        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return JsonSerializer.Deserialize(data, GetRequiredTypeInfo(messageType));
    }

    private JsonTypeInfo GetRequiredTypeInfo(Type type)
    {
        if (_serializerOptions.TryGetTypeInfo(type, out var typeInfo))
        {
            return typeInfo;
        }

        throw new InvalidOperationException(
            $"JsonTypeInfo metadata for type '{type}' was not provided by TypeInfoResolver of type '{GetResolverDisplayName()}'. If using source generation, ensure that all root types passed to the codec have been annotated with 'JsonSerializableAttribute', along with any types that might be serialized polymorphically."
        );
    }

    private string GetResolverDisplayName()
    {
        return _serializerOptions.TypeInfoResolver?.GetType().FullName ?? "<null>";
    }

    // Mirrors Microsoft.AspNetCore.Http.JsonSerializerExtensions so runtime-type selection stays aligned
    // with the serializer's declared-type polymorphism behavior.
    private static bool ShouldUseWith(JsonTypeInfo declaredTypeInfo, Type runtimeType) =>
        declaredTypeInfo.Type == runtimeType || HasKnownPolymorphism(declaredTypeInfo);

    private static bool HasKnownPolymorphism(JsonTypeInfo declaredTypeInfo) =>
        declaredTypeInfo.Type.IsSealed ||
        declaredTypeInfo.Type.IsValueType ||
        declaredTypeInfo.PolymorphismOptions is not null;

    private static JsonSerializerOptions CreateDefaultSerializerOptions() => JsonSerializerOptions.Default;
}
