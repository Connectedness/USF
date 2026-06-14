using System;

namespace Usf.Core.Messaging;

public sealed record InboundMessageInspectionResult(string Discriminator, Type MessageType)
{
    public InboundMessageInspectionResult(string discriminator, Type messageType, CloudEventEnvelope envelope)
        : this(discriminator, messageType)
    {
        Envelope = envelope;
    }

    public CloudEventEnvelope? Envelope { get; init; }

    public object? Message { get; init; }
}
