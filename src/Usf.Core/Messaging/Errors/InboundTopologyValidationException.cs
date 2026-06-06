using System;
using System.Collections.Generic;
using System.Linq;

namespace Usf.Core.Messaging.Errors;

public sealed class InboundTopologyValidationException : Exception
{
    public InboundTopologyValidationException(IReadOnlyList<string> validationErrors)
        : base(CreateMessage(validationErrors))
    {
        ValidationErrors = validationErrors ?? throw new ArgumentNullException(nameof(validationErrors));
    }

    public IReadOnlyList<string> ValidationErrors { get; }

    private static string CreateMessage(IReadOnlyList<string> validationErrors)
    {
        if (validationErrors is null)
        {
            throw new ArgumentNullException(nameof(validationErrors));
        }

        if (validationErrors.Count == 0)
        {
            throw new ArgumentException("At least one validation error is required.", nameof(validationErrors));
        }

        return "Inbound topology validation failed: " +
               string.Join(" ", validationErrors.OrderBy(static error => error, StringComparer.Ordinal));
    }
}
