using System;
using Usf.Abstractions;

namespace Usf.Core.Tests.Messaging.TestSupport;

public record BaseSampleMessage(string Value) : ICloudEvent
{
    Guid ICloudEvent.Id { get; } = UsfUuid.NewId();

    DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

    string? ICloudEvent.Subject => null;
}
