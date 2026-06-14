using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class CloudEventsInboundMessageInspector : IInboundMessageInspector
{
    public const string CloudEventsHeaderPrefix = "cloudEvents:";

    private readonly IMessageContractRegistry _messageContractRegistry;

    public CloudEventsInboundMessageInspector(IMessageContractRegistry messageContractRegistry)
    {
        _messageContractRegistry = messageContractRegistry ??
                                   throw new ArgumentNullException(nameof(messageContractRegistry));
    }

    public ValueTask<InboundMessageInspectionResult> InspectAsync(
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    )
    {
        if (transportMessage is null)
        {
            throw new ArgumentNullException(nameof(transportMessage));
        }

        var type = GetRequiredHeader(transportMessage, CloudEventAttributeNames.Type);

        if (!_messageContractRegistry.TryResolveType(type, out var messageType) || messageType is null)
        {
            throw new UnknownInboundMessageException(
                transportMessage.Source,
                type,
                $"No inbound message contract is registered for CloudEvents type '{type}'."
            );
        }

        var envelope = new CloudEventEnvelope(
            GetRequiredHeader(transportMessage, CloudEventAttributeNames.SpecVersion),
            GetRequiredHeader(transportMessage, CloudEventAttributeNames.Id),
            GetRequiredHeader(transportMessage, CloudEventAttributeNames.Source),
            type,
            ParseTime(GetRequiredHeader(transportMessage, CloudEventAttributeNames.Time)),
            GetOptionalHeader(transportMessage, CloudEventAttributeNames.Subject),
            transportMessage.ContentType ??
            GetOptionalHeader(transportMessage, CloudEventAttributeNames.DataContentType) ?? "application/octet-stream",
            GetOptionalHeader(transportMessage, CloudEventAttributeNames.DataSchema),
            transportMessage.Body,
            GetExtensions(transportMessage)
        );

        return new ValueTask<InboundMessageInspectionResult>(
            new InboundMessageInspectionResult(type, messageType, envelope)
        );
    }

    private static IReadOnlyDictionary<string, string?>? GetExtensions(TransportMessage transportMessage)
    {
        Dictionary<string, string?>? extensions = null;

        foreach (var header in transportMessage.Headers)
        {
            if (!header.Key.StartsWith(CloudEventsHeaderPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var attributeName = header.Key.Substring(CloudEventsHeaderPrefix.Length);

            if (IsCoreAttribute(attributeName))
            {
                continue;
            }

            if (!transportMessage.TryGetHeaderString(header.Key, out var value))
            {
                continue;
            }

            extensions ??= new Dictionary<string, string?>(StringComparer.Ordinal);
            extensions[attributeName] = value;
        }

        return extensions;
    }

    private static bool IsCoreAttribute(string attributeName)
    {
        return string.Equals(attributeName, CloudEventAttributeNames.Id, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.SpecVersion, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.Source, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.Type, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.Time, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.Subject, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.DataContentType, StringComparison.Ordinal) ||
               string.Equals(attributeName, CloudEventAttributeNames.DataSchema, StringComparison.Ordinal);
    }

    private static string GetRequiredHeader(TransportMessage transportMessage, string attributeName)
    {
        var headerName = GetHeaderName(attributeName);

        if (transportMessage.TryGetHeaderString(headerName, out var value) &&
            !string.IsNullOrWhiteSpace(value))
        {
            return value!;
        }

        throw new InvalidOperationException($"CloudEvents attribute '{attributeName}' is missing.");
    }

    private static string? GetOptionalHeader(TransportMessage transportMessage, string attributeName)
    {
        return transportMessage.TryGetHeaderString(GetHeaderName(attributeName), out var value) ? value : null;
    }

    private static string GetHeaderName(string attributeName)
    {
        return $"{CloudEventsHeaderPrefix}{attributeName}";
    }

    private static DateTimeOffset ParseTime(string value)
    {
        return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    }
}
