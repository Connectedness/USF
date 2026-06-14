using System;
using System.Collections.Generic;
using System.Linq;

namespace Usf.Core.Messaging.Errors;

/// <summary>
/// Reports that a topology failed compile-time validation. The exception type is direction-neutral, but the
/// individual validation messages remain precise and name the failed transport feature and direction where
/// relevant, such as "RabbitMQ outbound target ...", "RabbitMQ inbound endpoint ...", "RabbitMQ exchange ...",
/// or "RabbitMQ consumer channel group ...".
/// </summary>
public sealed class TopologyValidationException : Exception
{
    public TopologyValidationException(IReadOnlyList<string> validationErrors)
        : base("Topology validation failed.")
    {
        if (validationErrors is null)
        {
            throw new ArgumentNullException(nameof(validationErrors));
        }

        if (validationErrors.Count == 0)
        {
            throw new ArgumentException("At least one validation error must be provided.", nameof(validationErrors));
        }

        ValidationErrors = Array.AsReadOnly(
            validationErrors.OrderBy(static error => error, StringComparer.Ordinal).ToArray()
        );
    }

    public IReadOnlyList<string> ValidationErrors { get; }
}
