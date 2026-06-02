using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Usf.Core.Messaging;

/// <summary>
/// Injects the current distributed trace context into transport headers.
/// </summary>
/// <remarks>
/// The injected <c>traceparent</c>, <c>tracestate</c>, and baggage headers are W3C transport-propagation
/// headers used by the OpenTelemetry messaging convention. They are intentionally plain and un-prefixed,
/// rather than CloudEvents attributes. The CloudEvents Distributed Tracing extension captures creation-time
/// provenance and is intentionally deferred to a later slice. Raw-publish callers can use this helper to opt
/// in before constructing a <see cref="SerializedMessage" />.
/// </remarks>
public static class TraceContextHeaders
{
    /// <summary>
    /// Injects trace-context headers into a string-valued carrier.
    /// </summary>
    /// <param name="headers">The transport headers to populate.</param>
    /// <param name="activity">The activity to inject, or <see langword="null" /> to use <see cref="Activity.Current" />.</param>
    public static void Inject(IDictionary<string, string?> headers, Activity? activity = null)
    {
        if (headers is null)
        {
            throw new ArgumentNullException(nameof(headers));
        }

        DistributedContextPropagator.Current.Inject(
            activity ?? Activity.Current,
            headers,
            static (carrier, key, value) => ((IDictionary<string, string?>) carrier!)[key] = value
        );
    }

    /// <summary>
    /// Injects trace-context headers into an object-valued carrier.
    /// </summary>
    /// <param name="headers">The transport headers to populate.</param>
    /// <param name="activity">The activity to inject, or <see langword="null" /> to use <see cref="Activity.Current" />.</param>
    public static void Inject(IDictionary<string, object?> headers, Activity? activity = null)
    {
        if (headers is null)
        {
            throw new ArgumentNullException(nameof(headers));
        }

        DistributedContextPropagator.Current.Inject(
            activity ?? Activity.Current,
            headers,
            static (carrier, key, value) => ((IDictionary<string, object?>) carrier!)[key] = value
        );
    }
}
