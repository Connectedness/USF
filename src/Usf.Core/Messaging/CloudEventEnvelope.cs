using System;
using System.Collections.Generic;
using Generator.Equals;

namespace Usf.Core.Messaging;

/// <summary>
/// Represents a transport-neutral CloudEvents v1.0 envelope in binary content mode.
/// </summary>
/// <remarks>
/// A transport binds these attributes according to its protocol binding. The RabbitMQ transport uses the AMQP
/// protocol binding over AMQP 0.9.1.
/// </remarks>
[Equatable]
public readonly partial record struct CloudEventEnvelope(
    string SpecVersion,
    string Id,
    string Source,
    string Type,
    DateTimeOffset Time,
    string? Subject,
    string DataContentType,
    string? DataSchema,
    [property: CustomEquality(typeof(ReadOnlyMemoryByteEqualityComparer))] ReadOnlyMemory<byte> Data,
    [property: UnorderedEquality] IReadOnlyDictionary<string, string?>? Extensions = null
);
