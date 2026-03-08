using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public sealed class MessagePublisher : IMessagePublisher
{
    private readonly IMessageTopology _messageTopology;

    public MessagePublisher(IMessageTopology messageTopology)
    {
        _messageTopology = messageTopology ?? throw new ArgumentNullException(nameof(messageTopology));
    }

    public async Task PublishMessageAsync<T>(
        T message,
        Target? target = null,
        CancellationToken cancellationToken = default
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var resolvedTarget = target ?? _messageTopology.GetRequiredTarget<T>();
        var messageTypeName = GetMessageTypeName(typeof(T));
        var tags = CreateBaseTags(messageTypeName, resolvedTarget.Name, resolvedTarget.TransportName);
        var activity = MessagePublishingDiagnostics.ActivitySource.StartActivity(
            "usf.messaging.publish",
            ActivityKind.Producer
        );
        var startedTimestamp = Stopwatch.GetTimestamp();

        SetCommonTags(activity, messageTypeName, resolvedTarget.Name, resolvedTarget.TransportName);
        MessagePublishingDiagnostics.PublishAttempts.Add(1, tags);

        var outcome = "success";

        try
        {
            if (resolvedTarget is Target<T> typedTarget)
            {
                await typedTarget.PublishAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await resolvedTarget.PublishUntypedAsync(message, cancellationToken).ConfigureAwait(false);
            }

            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException)
        {
            outcome = "cancelled";
            activity?.SetTag(MessagePublishingDiagnostics.OutcomeTagName, outcome);
            throw;
        }
        catch
        {
            outcome = "failure";
            MessagePublishingDiagnostics.PublishFailures.Add(
                1,
                CreateBaseTags(messageTypeName, resolvedTarget.Name, resolvedTarget.TransportName, outcome)
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(MessagePublishingDiagnostics.OutcomeTagName, outcome);
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
            MessagePublishingDiagnostics.PublishDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(MessagePublishingDiagnostics.OutcomeTagName, outcome);
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
                new KeyValuePair<string, object?>(MessagePublishingDiagnostics.MessageTypeTagName, messageTypeName),
                new KeyValuePair<string, object?>(MessagePublishingDiagnostics.TargetNameTagName, targetName),
                new KeyValuePair<string, object?>(MessagePublishingDiagnostics.TransportNameTagName, transportName)
            ];
        }

        return
        [
            new KeyValuePair<string, object?>(MessagePublishingDiagnostics.MessageTypeTagName, messageTypeName),
            new KeyValuePair<string, object?>(MessagePublishingDiagnostics.TargetNameTagName, targetName),
            new KeyValuePair<string, object?>(MessagePublishingDiagnostics.TransportNameTagName, transportName),
            new KeyValuePair<string, object?>(MessagePublishingDiagnostics.OutcomeTagName, outcome)
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
        activity?.SetTag(MessagePublishingDiagnostics.MessageTypeTagName, messageTypeName);
        activity?.SetTag(MessagePublishingDiagnostics.TargetNameTagName, targetName);
        activity?.SetTag(MessagePublishingDiagnostics.TransportNameTagName, transportName);
    }
}
