using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundTopologyProvisioner : ITopologyProvisioner
{
    private readonly RabbitMqInboundTopology _inboundTopology;

    public RabbitMqInboundTopologyProvisioner(RabbitMqInboundTopology inboundTopology)
    {
        _inboundTopology = inboundTopology ?? throw new ArgumentNullException(nameof(inboundTopology));
    }

    public async Task ProvisionAsync(CancellationToken cancellationToken = default)
    {
        await using var channel = await _inboundTopology.CreateChannelAsync(cancellationToken)
           .ConfigureAwait(false);

        foreach (var exchange in _inboundTopology.Exchanges)
        {
            await ProvisionExchangeAsync(channel, exchange, cancellationToken).ConfigureAwait(false);
        }

        foreach (var queue in _inboundTopology.Queues)
        {
            await ProvisionQueueAsync(channel, queue, cancellationToken).ConfigureAwait(false);
        }

        foreach (var binding in _inboundTopology.Bindings)
        {
            await ProvisionBindingAsync(channel, binding, cancellationToken).ConfigureAwait(false);
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
