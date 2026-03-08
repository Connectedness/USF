using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

internal sealed class RabbitMqTopologyProvisioner : ITopologyProvisioner
{
    private readonly RabbitMqPublishingConfiguration _configuration;
    private readonly RabbitMqConnectionManager _connectionManager;

    public RabbitMqTopologyProvisioner(
        RabbitMqPublishingConfiguration configuration,
        RabbitMqConnectionManager connectionManager
    )
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
    }

    public async Task ProvisionAsync(CancellationToken cancellationToken = default)
    {
        var outcome = "success";
        var activity = MessagePublishingDiagnostics.ActivitySource.StartActivity("usf.messaging.topology.provision");
        var startedTimestamp = Stopwatch.GetTimestamp();
        KeyValuePair<string, object?>[] attemptTags =
        [
            new (MessagePublishingDiagnostics.TransportNameTagName, "rabbitmq")
        ];

        MessagePublishingDiagnostics.TopologyProvisioningAttempts.Add(1, attemptTags);
        activity?.SetTag(MessagePublishingDiagnostics.TransportNameTagName, "rabbitmq");

        try
        {
            var connection = await _connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
               .ConfigureAwait(false);

            foreach (var exchange in _configuration.Exchanges)
            {
                await ProvisionExchangeAsync(channel, exchange, cancellationToken).ConfigureAwait(false);
            }

            foreach (var queue in _configuration.Queues)
            {
                await ProvisionQueueAsync(channel, queue, cancellationToken).ConfigureAwait(false);
            }

            foreach (var binding in _configuration.Bindings)
            {
                await ProvisionBindingAsync(channel, binding, cancellationToken).ConfigureAwait(false);
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
            MessagePublishingDiagnostics.TopologyProvisioningFailures.Add(
                1,
                new[]
                {
                    new KeyValuePair<string, object?>(MessagePublishingDiagnostics.TransportNameTagName, "rabbitmq"),
                    new KeyValuePair<string, object?>(MessagePublishingDiagnostics.OutcomeTagName, outcome)
                }
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(MessagePublishingDiagnostics.OutcomeTagName, outcome);
            throw;
        }
        finally
        {
            KeyValuePair<string, object?>[] durationTags =
            [
                new (MessagePublishingDiagnostics.TransportNameTagName, "rabbitmq"),
                new (MessagePublishingDiagnostics.OutcomeTagName, outcome)
            ];
            var durationMilliseconds = (Stopwatch.GetTimestamp() - startedTimestamp) * 1000d / Stopwatch.Frequency;
            MessagePublishingDiagnostics.TopologyProvisioningDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(MessagePublishingDiagnostics.OutcomeTagName, outcome);
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

        return binding.DeclareMode switch
        {
            RabbitMqDeclareMode.None => Task.CompletedTask,
            RabbitMqDeclareMode.Passive => channel.QueueBindAsync(
                binding.QueueName,
                binding.ExchangeName,
                binding.RoutingKey,
                arguments,
                false,
                cancellationToken
            ),
            RabbitMqDeclareMode.Active => channel.QueueBindAsync(
                binding.QueueName,
                binding.ExchangeName,
                binding.RoutingKey,
                arguments,
                false,
                cancellationToken
            ),
            _ => throw new ArgumentOutOfRangeException(
                nameof(binding),
                binding.DeclareMode,
                "Unsupported declare mode."
            )
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
            RabbitMqDeclareMode.None => Task.CompletedTask,
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
            RabbitMqDeclareMode.None => Task.CompletedTask,
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
