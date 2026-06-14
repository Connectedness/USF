using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class MessageContractRegistry : IMessageContractRegistry
{
    private readonly IReadOnlyDictionary<Type, string> _dataSchemasByMessageType;
    private readonly IReadOnlyDictionary<Type, string> _discriminatorsByMessageType;
    private readonly IReadOnlyDictionary<string, Type> _messageTypesByDiscriminator;

    public MessageContractRegistry(
        IDictionary<Type, string> discriminatorsByMessageType,
        IDictionary<string, Type> messageTypesByDiscriminator,
        IDictionary<Type, string> dataSchemasByMessageType
    )
    {
        if (discriminatorsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(discriminatorsByMessageType));
        }

        if (messageTypesByDiscriminator is null)
        {
            throw new ArgumentNullException(nameof(messageTypesByDiscriminator));
        }

        if (dataSchemasByMessageType is null)
        {
            throw new ArgumentNullException(nameof(dataSchemasByMessageType));
        }

        _discriminatorsByMessageType = new ReadOnlyDictionary<Type, string>(
            new Dictionary<Type, string>(discriminatorsByMessageType)
        );
        _messageTypesByDiscriminator = new ReadOnlyDictionary<string, Type>(
            new Dictionary<string, Type>(messageTypesByDiscriminator, StringComparer.Ordinal)
        );
        _dataSchemasByMessageType = new ReadOnlyDictionary<Type, string>(
            new Dictionary<Type, string>(dataSchemasByMessageType)
        );
        RegisteredMessageTypes = _discriminatorsByMessageType.Keys.OrderBy(
            static messageType => messageType.FullName ?? messageType.Name,
            StringComparer.Ordinal
        ).ToArray();
    }

    public IReadOnlyCollection<Type> RegisteredMessageTypes { get; }

    public string GetDiscriminator(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        if (!_discriminatorsByMessageType.TryGetValue(messageType, out var discriminator))
        {
            throw new MessageContractNotRegisteredException(messageType);
        }

        return discriminator;
    }

    public bool TryGetDiscriminator(Type messageType, out string? discriminator)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _discriminatorsByMessageType.TryGetValue(messageType, out discriminator);
    }

    public string? GetDataSchema(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _dataSchemasByMessageType.TryGetValue(messageType, out var dataSchema) ? dataSchema : null;
    }

    public IReadOnlyCollection<string> GetInboundDiscriminators(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _messageTypesByDiscriminator
           .Where(pair => pair.Value == messageType)
           .Select(static pair => pair.Key)
           .OrderBy(static discriminator => discriminator, StringComparer.Ordinal)
           .ToArray();
    }

    public bool TryResolveType(string discriminator, out Type? messageType)
    {
        if (string.IsNullOrWhiteSpace(discriminator))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(discriminator));
        }

        return _messageTypesByDiscriminator.TryGetValue(discriminator, out messageType);
    }

    public bool TryGetDataSchema(Type messageType, out string? dataSchema)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _dataSchemasByMessageType.TryGetValue(messageType, out dataSchema);
    }
}
