using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class MessagePublisher : IMessagePublisher
{
    private const string SerializedMessageTypeName = "serialized";

    private readonly IOutboundTopology _outboundTopology;

    public MessagePublisher(IOutboundTopology outboundTopology)
    {
        _outboundTopology = outboundTopology ?? throw new ArgumentNullException(nameof(outboundTopology));
    }

    public async Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var resolvedTarget = target ?? _outboundTopology.GetRequiredTarget<T>();
        var messageTypeName = GetMessageTypeName(typeof(T));
        await PublishWithDiagnosticsAsync(
            "usf.outbound.publish",
            messageTypeName,
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

                await typedTarget.PublishAsync(message, cancellationToken).ConfigureAwait(false);
            }
        ).ConfigureAwait(false);
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
        catch
        {
            outcome = "failure";
            OutboundDiagnostics.PublishFailures.Add(
                1,
                CreateBaseTags(messageTypeName, resolvedTarget.Name, resolvedTarget.TransportName, outcome)
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            throw;
        }
        finally
        {
            var durationMilliseconds = GetDurationMilliseconds(startedTimestamp);
            var durationTags = CreateBaseTags(
                messageTypeName,
                resolvedTarget.Name,
                resolvedTarget.TransportName,
                outcome
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
        string? outcome = null
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

        return
        [
            new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, outcome)
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
