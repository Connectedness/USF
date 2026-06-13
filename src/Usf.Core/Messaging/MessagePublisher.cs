using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class MessagePublisher : IMessagePublisher
{
    private const string SerializedMessageTypeName = "serialized";

    private readonly IOutboundTopologyRegistry _outboundTopologyRegistry;

    [ActivatorUtilitiesConstructor]
    public MessagePublisher(IOutboundTopologyRegistry outboundTopologyRegistry)
    {
        _outboundTopologyRegistry = outboundTopologyRegistry ??
                                    throw new ArgumentNullException(nameof(outboundTopologyRegistry));
    }

    public MessagePublisher(IOutboundTopology outboundTopology)
        : this(new SingleOutboundTopologyRegistry(outboundTopology)) { }

    public TopologyPublisher ForTopology(TopologyName topologyName)
    {
        return new TopologyPublisher(this, topologyName);
    }

    public Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var metadata = CloudEventMetadata.From(message);
        return PublishMessageAsync(message, in metadata, TopologyName.Default, target, routingKey, cancellationToken);
    }

    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        return PublishMessageAsync(message, in metadata, TopologyName.Default, target, routingKey, cancellationToken);
    }

    public async Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    )
    {
        await PublishRawAsync(message, target, TopologyName.Default, cancellationToken).ConfigureAwait(false);
    }

    public Task PublishMessageAsync<T>(
        T message,
        TopologyName topologyName,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var metadata = CloudEventMetadata.From(message);
        return PublishMessageAsync(message, in metadata, topologyName, target, routingKey, cancellationToken);
    }

    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        TopologyName topologyName,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        return PublishMessageCoreAsync(message, metadata, target, topologyName, routingKey, cancellationToken);
    }

    public async Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        TopologyName topologyName,
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

        ValidateExplicitTargetTopology(target, topologyName);
        await PublishWithDiagnosticsAsync(
            "usf.outbound.publish",
            GetMessageTypeName(target),
            target,
            async () => await target.PublishSerializedAsync(message, cancellationToken).ConfigureAwait(false)
        ).ConfigureAwait(false);
    }

    private async Task PublishMessageCoreAsync<T>(
        T message,
        CloudEventMetadata metadata,
        OutboundTarget? target,
        TopologyName topologyName,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var resolvedTarget = target ??
                             _outboundTopologyRegistry
                                .GetRequiredTopology(topologyName)
                                .GetRequiredTarget<T>();
        ValidateExplicitTargetTopology(resolvedTarget, topologyName, target is not null);
        if (resolvedTarget is not OutboundTarget<T> typedTarget)
        {
            throw new OutboundTargetTypeMismatchException(
                resolvedTarget.Name,
                typeof(T),
                resolvedTarget.MessageType
            );
        }

        var runtimeType = message.GetType();
        var discriminator = typedTarget.GetRequiredDiscriminator(runtimeType);
        var dataSchema = typedTarget.GetDataSchema(runtimeType);
        await PublishWithDiagnosticsAsync(
            "usf.outbound.publish",
            discriminator,
            resolvedTarget,
            async () => await typedTarget.PublishAsync(
                    message,
                    in metadata,
                    discriminator,
                    dataSchema,
                    routingKey,
                    cancellationToken
                )
               .ConfigureAwait(false)
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

    private static string GetMessageTypeName(OutboundTarget target)
    {
        if (target.MessageType is null)
        {
            return SerializedMessageTypeName;
        }

        return target.GetDiagnosticMessageTypeName(target.MessageType);
    }

    private static void ValidateExplicitTargetTopology(
        OutboundTarget target,
        TopologyName topologyName,
        bool hasExplicitTarget = true
    )
    {
        if (!hasExplicitTarget || topologyName == TopologyName.Default || target.TopologyName == topologyName)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Outbound target '{target.Name}' belongs to outbound topology '{target.TopologyName.Value}', but publish requested outbound topology '{topologyName.Value}'."
        );
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
