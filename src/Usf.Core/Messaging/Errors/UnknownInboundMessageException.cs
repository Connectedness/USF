using System;

namespace Usf.Core.Messaging.Errors;

public sealed class UnknownInboundMessageException : Exception
{
    public UnknownInboundMessageException(string source, string discriminator, string? message = null)
        : base(message ?? $"Inbound message discriminator '{discriminator}' from '{source}' is not registered.")
    {
        TransportSource = source;
        Discriminator = discriminator;
    }

    public string TransportSource { get; }

    public string Discriminator { get; }
}
