using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class MessagePublisher : IMessagePublisher
{
    private const string SerializedMessageTypeName = "serialized";

    private readonly IMessageContractRegistry _messageContractRegistry;
    private readonly IOutboundTopology _outboundTopology;

    public MessagePublisher(IOutboundTopology outboundTopology, IMessageContractRegistry messageContractRegistry)
    {
        _outboundTopology = outboundTopology ?? throw new ArgumentNullException(nameof(outboundTopology));
        _messageContractRegistry = messageContractRegistry ??
                                   throw new ArgumentNullException(nameof(messageContractRegistry));
    }

    public Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var metadata = CloudEventMetadata.From(message);
        return PublishMessageAsync(message, in metadata, target, cancellationToken);
    }

    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    )
    {
        return PublishMessageCoreAsync(message, metadata, target, cancellationToken);
    }

    public async Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    )
    {
        if (target is null)
        {
            throw new ArgumentNullException(nameof(target));
        }

        if (message.Body is null)
        {
            throw new ArgumentException("The serialized message must provide a body.", nameof(message));
        }

        if (message.Headers is null)
        {
            throw new ArgumentException("The serialized message must provide headers.", nameof(message));
        }

        var messageTypeName = target.MessageType is null ?
            SerializedMessageTypeName :
            GetMessageTypeName(target.MessageType);
        await PublishWithDiagnosticsAsync(
            "usf.outbound.publish",
            messageTypeName,
            target,
            async () => await target.PublishSerializedAsync(message, cancellationToken).ConfigureAwait(false)
        ).ConfigureAwait(false);
    }

    private async Task PublishMessageCoreAsync<T>(
        T message,
        CloudEventMetadata metadata,
        OutboundTarget? target,
        CancellationToken cancellationToken
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var resolvedTarget = target ?? _outboundTopology.GetRequiredTarget<T>();
        var discriminator = GetRequiredDiscriminator(message.GetType());
        await PublishWithDiagnosticsAsync(
            "usf.outbound.publish",
            discriminator,
            resolvedTarget,
            async () =>
            {
                if (resolvedTarget is not OutboundTarget<T> typedTarget)
                {
                    throw new OutboundTargetTypeMismatchException(
                        resolvedTarget.Name,
                        typeof(T),
                        resolvedTarget.MessageType
                    );
                }

                await typedTarget.PublishAsync(message, in metadata, discriminator, cancellationToken)
                   .ConfigureAwait(false);
            }
        ).ConfigureAwait(false);
    }

    private static async Task PublishWithDiagnosticsAsync(
        string activityName,
        string messageTypeName,
        OutboundTarget resolvedTarget,
        Func<Task> publishAsync
    )
    {
        var tags = CreateBaseTags(messageTypeName, resolvedTarget.Name, resolvedTarget.TransportName);
        var activity = OutboundDiagnostics.ActivitySource.StartActivity(
            activityName,
            ActivityKind.Producer
        );
        var startedTimestamp = Stopwatch.GetTimestamp();

        SetCommonTags(activity, messageTypeName, resolvedTarget.Name, resolvedTarget.TransportName);
        OutboundDiagnostics.PublishAttempts.Add(1, tags);

        var outcome = "success";
        string? deliveryFailureReason = null;

        try
        {
            await publishAsync().ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException)
        {
            outcome = "cancelled";
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            throw;
        }
        catch (Exception exception)
        {
            outcome = "failure";
            deliveryFailureReason = exception is MessageDeliveryException deliveryException ?
                GetDeliveryFailureReasonName(deliveryException.Reason) :
                null;
            OutboundDiagnostics.PublishFailures.Add(
                1,
                CreateBaseTags(
                    messageTypeName,
                    resolvedTarget.Name,
                    resolvedTarget.TransportName,
                    outcome,
                    deliveryFailureReason
                )
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            activity?.SetTag(OutboundDiagnostics.DeliveryFailureReasonTagName, deliveryFailureReason);
            throw;
        }
        finally
        {
            var durationMilliseconds = GetDurationMilliseconds(startedTimestamp);
            var durationTags = CreateBaseTags(
                messageTypeName,
                resolvedTarget.Name,
                resolvedTarget.TransportName,
                outcome,
                deliveryFailureReason
            );
            OutboundDiagnostics.PublishDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            activity?.Dispose();
        }
    }

    private static KeyValuePair<string, object?>[] CreateBaseTags(
        string messageTypeName,
        string targetName,
        string transportName,
        string? outcome = null,
        string? deliveryFailureReason = null
    )
    {
        if (outcome is null)
        {
            return
            [
                new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName)
            ];
        }

        if (deliveryFailureReason is null)
        {
            return
            [
                new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, outcome)
            ];
        }

        return
        [
            new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, outcome),
            new KeyValuePair<string, object?>(
                OutboundDiagnostics.DeliveryFailureReasonTagName,
                deliveryFailureReason
            )
        ];
    }

    private static double GetDurationMilliseconds(long startedTimestamp)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startedTimestamp;
        return elapsedTicks * 1000d / Stopwatch.Frequency;
    }

    private static string GetMessageTypeName(Type messageType)
    {
        return messageType.FullName ?? messageType.Name;
    }

    private string GetRequiredDiscriminator(Type messageType)
    {
        try
        {
            return _messageContractRegistry.GetDiscriminator(messageType);
        }
        catch (MessageContractNotRegisteredException exception)
        {
            throw new CloudEventMetadataException(
                "type",
                $"Register the runtime message type '{exception.MessageType}' with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
            );
        }
    }

    private static string GetDeliveryFailureReasonName(MessageDeliveryFailureReason reason)
    {
        return reason switch
        {
            MessageDeliveryFailureReason.Nacked => "nacked",
            MessageDeliveryFailureReason.Returned => "returned",
            MessageDeliveryFailureReason.Timeout => "timeout",
            _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unsupported delivery-failure reason.")
        };
    }

    private static void SetCommonTags(
        Activity? activity,
        string messageTypeName,
        string targetName,
        string transportName
    )
    {
        activity?.SetTag(OutboundDiagnostics.MessageTypeTagName, messageTypeName);
        activity?.SetTag(OutboundDiagnostics.TargetNameTagName, targetName);
        activity?.SetTag(OutboundDiagnostics.TransportNameTagName, transportName);
    }
}
