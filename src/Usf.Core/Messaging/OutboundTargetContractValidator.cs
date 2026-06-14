using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

/// <summary>
/// Cross-checks outbound targets against the message-contract registry. Each transport runs this while validating
/// its topology so that an outbound target publishing an unregistered CloudEvents message type is reported
/// together with the transport's own validation errors and fails fast at compile time, aggregated into a single
/// exception.
/// </summary>
public static class OutboundTargetContractValidator
{
    /// <summary>
    /// Adds a validation error for every typed outbound target whose message type has no canonical CloudEvents
    /// discriminator registered.
    /// </summary>
    /// <param name="messageContractRegistry">The registry that maps message types to discriminators.</param>
    /// <param name="typedTargets">The typed outbound targets as (target name, message type) pairs.</param>
    /// <param name="validationErrors">The collection that receives the validation errors.</param>
    public static void CollectValidationErrors(
        IMessageContractRegistry messageContractRegistry,
        IEnumerable<KeyValuePair<string, Type>> typedTargets,
        ICollection<string> validationErrors
    )
    {
        if (messageContractRegistry is null)
        {
            throw new ArgumentNullException(nameof(messageContractRegistry));
        }

        if (typedTargets is null)
        {
            throw new ArgumentNullException(nameof(typedTargets));
        }

        if (validationErrors is null)
        {
            throw new ArgumentNullException(nameof(validationErrors));
        }

        foreach (var typedTarget in typedTargets)
        {
            if (!messageContractRegistry.TryGetDiscriminator(typedTarget.Value, out _))
            {
                validationErrors.Add(
                    $"Outbound target '{typedTarget.Key}' publishes unregistered CloudEvents message type '{typedTarget.Value}'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
                );
            }
        }
    }
}
