using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

/// <summary>
/// Resolves CloudEvents type discriminators without coupling wire contracts to CLR type names.
/// </summary>
/// <remarks>
/// The mapping is intentionally asymmetric: serialization resolves one canonical discriminator per CLR type,
/// while deserialization may resolve multiple discriminators to one CLR type. This permits old wire names to
/// remain accepted after a backwards-compatible rename.
/// </remarks>
public interface IMessageContractRegistry
{
    string GetDiscriminator(Type messageType);

    bool TryGetDiscriminator(Type messageType, out string? discriminator);

    string? GetDataSchema(Type messageType);

    IReadOnlyCollection<string> GetInboundDiscriminators(Type messageType);

    bool TryResolveType(string discriminator, out Type? messageType);
}
