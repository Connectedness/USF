using System;
using System.Collections.Generic;
using System.Linq;

namespace Usf.Core.Messaging;

public sealed class EffectiveMessageContractRegistry : IMessageContractRegistry
{
    private readonly IMessageContractRegistry _canonical;
    private readonly MessageContractRegistry _dialect;

    public EffectiveMessageContractRegistry(IMessageContractRegistry canonical, MessageContractRegistry dialect)
    {
        _canonical = canonical ?? throw new ArgumentNullException(nameof(canonical));
        _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    }

    public string GetDiscriminator(Type messageType)
    {
        return _dialect.TryGetDiscriminator(messageType, out var discriminator) ?
            discriminator! :
            _canonical.GetDiscriminator(messageType);
    }

    public bool TryGetDiscriminator(Type messageType, out string? discriminator)
    {
        return _dialect.TryGetDiscriminator(messageType, out discriminator) ||
               _canonical.TryGetDiscriminator(messageType, out discriminator);
    }

    public string? GetDataSchema(Type messageType)
    {
        return _dialect.TryGetDataSchema(messageType, out var dataSchema) ?
            dataSchema :
            _canonical.GetDataSchema(messageType);
    }

    public IReadOnlyCollection<string> GetInboundDiscriminators(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _dialect.GetInboundDiscriminators(messageType)
           .Concat(_canonical.GetInboundDiscriminators(messageType))
           .Distinct(StringComparer.Ordinal)
           .OrderBy(static discriminator => discriminator, StringComparer.Ordinal)
           .ToArray();
    }

    public bool TryResolveType(string discriminator, out Type? messageType)
    {
        return _dialect.TryResolveType(discriminator, out messageType) ||
               _canonical.TryResolveType(discriminator, out messageType);
    }
}
