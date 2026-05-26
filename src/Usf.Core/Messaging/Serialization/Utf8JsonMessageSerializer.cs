using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging.Serialization;

public sealed class Utf8JsonMessageSerializer : IMessageSerializer
{
    private static readonly IReadOnlyDictionary<string, string?> EmptyHeaders =
        new Dictionary<string, string?>(0, StringComparer.Ordinal);

    private static readonly JsonSerializerOptions DefaultSerializerOptions = CreateDefaultSerializerOptions();

    private readonly JsonSerializerOptions _serializerOptions;

    public Utf8JsonMessageSerializer()
        : this(DefaultSerializerOptions) { }

    public Utf8JsonMessageSerializer(JsonSerializerOptions serializerOptions)
    {
        _serializerOptions = serializerOptions is null ?
            throw new ArgumentNullException(nameof(serializerOptions)) :
            new JsonSerializerOptions(serializerOptions);
    }

    public ValueTask<SerializedMessage> SerializeAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var declaredType = typeof(T);
        var runtimeType = message.GetType();
        var typeInfo = GetRequiredTypeInfo(declaredType);

        if (declaredType != runtimeType && !DeclaredTypeMetadataSupportsRuntimeType(typeInfo, runtimeType))
        {
            typeInfo = GetRequiredTypeInfo(runtimeType);
        }

        var utf8Bytes = JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);
        SerializedMessage serializedMessage = new (
            utf8Bytes,
            "application/json",
            "utf-8",
            EmptyHeaders,
            null,
            null
        );

        return new ValueTask<SerializedMessage>(serializedMessage);
    }

    private JsonTypeInfo GetRequiredTypeInfo(Type type)
    {
        if (_serializerOptions.TryGetTypeInfo(type, out var typeInfo) && typeInfo is not null)
        {
            return typeInfo;
        }

        throw new InvalidOperationException(
            $"JsonTypeInfo metadata for type '{type}' was not provided by TypeInfoResolver of type '{GetResolverDisplayName()}'. " +
            "If using source generation, ensure that all root types passed to the serializer have been annotated with " +
            "'JsonSerializableAttribute', along with any types that might be serialized polymorphically."
        );
    }

    private string GetResolverDisplayName()
    {
        return _serializerOptions.TypeInfoResolver?.GetType().FullName ?? "<null>";
    }

    private static bool DeclaredTypeMetadataSupportsRuntimeType(JsonTypeInfo declaredTypeInfo, Type runtimeType)
    {
        var polymorphismOptions = declaredTypeInfo.PolymorphismOptions;

        if (polymorphismOptions is null)
        {
            return false;
        }

        foreach (var derivedType in polymorphismOptions.DerivedTypes)
        {
            if (derivedType.DerivedType == runtimeType)
            {
                return true;
            }
        }

        return false;
    }

    private static JsonSerializerOptions CreateDefaultSerializerOptions()
    {
        return new JsonSerializerOptions
        {
            TypeInfoResolver = new DefaultJsonTypeInfoResolver()
        };
    }
}
