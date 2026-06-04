using System;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public static class CloudEventsOptionsValidation
{
    public static string GetRequiredSource(CloudEventsOptions options)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        return GetRequiredSource(options.Source);
    }

    public static string GetRequiredSource(string? source)
    {
        if (!IsValidSource(source))
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Source,
                "Configure CloudEventsOptions.Source with a non-empty URI-reference or pass a per-call CloudEventMetadata.Source override."
            );
        }

        return source!;
    }

    public static bool IsValidSource(string? source)
    {
        return !string.IsNullOrWhiteSpace(source) && Uri.TryCreate(source, UriKind.RelativeOrAbsolute, out _);
    }
}
