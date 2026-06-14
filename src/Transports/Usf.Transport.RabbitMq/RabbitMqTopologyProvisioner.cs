using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

/// <summary>
/// Provisions the broker resources (exchanges, queues, and bindings) of a single <see cref="RabbitMqTopology" />.
/// It runs as an <see cref="ITopologyProvisioner" /> so that the shared topology-provisioning hosted service
/// declares all broker resources before any topology runtime starts.
/// </summary>
public sealed class RabbitMqTopologyProvisioner : ITopologyProvisioner
{
    private readonly RabbitMqTopology _topology;

    public RabbitMqTopologyProvisioner(RabbitMqTopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
    }

    public async Task ProvisionAsync(CancellationToken cancellationToken = default)
    {
        var outcome = "success";
        var activity = OutboundDiagnostics.ActivitySource.StartActivity("usf.outbound.topology.provision");
        var startedTimestamp = Stopwatch.GetTimestamp();
        KeyValuePair<string, object?>[] attemptTags =
        [
            new (OutboundDiagnostics.TransportNameTagName, "rabbitmq")
        ];

        OutboundDiagnostics.TopologyProvisioningAttempts.Add(1, attemptTags);
        activity?.SetTag(OutboundDiagnostics.TransportNameTagName, "rabbitmq");

        try
        {
            await using var channel = await _topology.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

            foreach (var exchange in _topology.Exchanges)
            {
                await ProvisionExchangeAsync(channel, exchange, cancellationToken).ConfigureAwait(false);
            }

            foreach (var queue in _topology.Queues)
            {
                await ProvisionQueueAsync(channel, queue, cancellationToken).ConfigureAwait(false);
            }

            foreach (var binding in _topology.Bindings)
            {
                await ProvisionBindingAsync(channel, binding, cancellationToken).ConfigureAwait(false);
            }

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
            OutboundDiagnostics.TopologyProvisioningFailures.Add(
                1,
                new[]
                {
                    new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, "rabbitmq"),
                    new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, outcome)
                }
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            throw;
        }
        finally
        {
            KeyValuePair<string, object?>[] durationTags =
            [
                new (OutboundDiagnostics.TransportNameTagName, "rabbitmq"),
                new (OutboundDiagnostics.OutcomeTagName, outcome)
            ];
            var durationMilliseconds = (Stopwatch.GetTimestamp() - startedTimestamp) * 1000d / Stopwatch.Frequency;
            OutboundDiagnostics.TopologyProvisioningDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            activity?.Dispose();
        }
    }

    private static Task ProvisionBindingAsync(
        IChannel channel,
        RabbitMqBindingDefinition binding,
        CancellationToken cancellationToken
    )
    {
        var arguments = CreateMutableArguments(binding.Arguments);

        return binding switch
        {
            RabbitMqQueueBindingDefinition { BindingMode: RabbitMqBindingMode.Skip } =>
                Task.CompletedTask,
            RabbitMqQueueBindingDefinition { BindingMode: RabbitMqBindingMode.Active } queueBinding =>
                channel.QueueBindAsync(
                    queueBinding.QueueName,
                    queueBinding.SourceExchangeName,
                    queueBinding.RoutingKey,
                    arguments,
                    false,
                    cancellationToken
                ),
            RabbitMqExchangeBindingDefinition { BindingMode: RabbitMqBindingMode.Skip } =>
                Task.CompletedTask,
            RabbitMqExchangeBindingDefinition { BindingMode: RabbitMqBindingMode.Active } exchangeBinding =>
                channel.ExchangeBindAsync(
                    exchangeBinding.DestinationExchangeName,
                    exchangeBinding.SourceExchangeName,
                    exchangeBinding.RoutingKey,
                    arguments,
                    false,
                    cancellationToken
                ),
            RabbitMqQueueBindingDefinition queueBinding => throw new ArgumentOutOfRangeException(
                nameof(binding),
                queueBinding.BindingMode,
                "Unsupported binding mode."
            ),
            RabbitMqExchangeBindingDefinition exchangeBinding => throw new ArgumentOutOfRangeException(
                nameof(binding),
                exchangeBinding.BindingMode,
                "Unsupported binding mode."
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(binding), binding, "Unsupported binding type.")
        };
    }

    private static Task ProvisionExchangeAsync(
        IChannel channel,
        RabbitMqExchangeDefinition exchange,
        CancellationToken cancellationToken
    )
    {
        var arguments = CreateMutableArguments(exchange.Arguments);

        return exchange.DeclareMode switch
        {
            RabbitMqDeclareMode.Skip => Task.CompletedTask,
            RabbitMqDeclareMode.Passive => channel.ExchangeDeclarePassiveAsync(exchange.Name, cancellationToken),
            RabbitMqDeclareMode.Active => channel.ExchangeDeclareAsync(
                exchange.Name,
                exchange.Type,
                exchange.Durable,
                exchange.AutoDelete,
                arguments,
                cancellationToken: cancellationToken
            ),
            _ => throw new ArgumentOutOfRangeException(
                nameof(exchange),
                exchange.DeclareMode,
                "Unsupported declare mode."
            )
        };
    }

    private static Task ProvisionQueueAsync(
        IChannel channel,
        RabbitMqQueueDefinition queue,
        CancellationToken cancellationToken
    )
    {
        var arguments = CreateMutableArguments(queue.Arguments);

        return queue.DeclareMode switch
        {
            RabbitMqDeclareMode.Skip => Task.CompletedTask,
            RabbitMqDeclareMode.Passive => channel.QueueDeclarePassiveAsync(queue.Name, cancellationToken),
            RabbitMqDeclareMode.Active => channel.QueueDeclareAsync(
                queue.Name,
                queue.Durable,
                queue.Exclusive,
                queue.AutoDelete,
                arguments,
                cancellationToken: cancellationToken
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(queue), queue.DeclareMode, "Unsupported declare mode.")
        };
    }

    private static IDictionary<string, object?> CreateMutableArguments(IReadOnlyDictionary<string, object?> arguments)
    {
        Dictionary<string, object?> mutableArguments = new (arguments.Count, StringComparer.Ordinal);

        foreach (var argument in arguments)
        {
            mutableArguments[argument.Key] = argument.Value;
        }

        return mutableArguments;
    }
}
