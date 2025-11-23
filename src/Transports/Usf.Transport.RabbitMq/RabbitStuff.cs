using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace Usf.Transport.RabbitMq;

public class MessageReceiver : AsyncDefaultBasicConsumer
{
    private readonly IServiceProvider _serviceProvider;

    public MessageReceiver(IChannel channel, IServiceProvider serviceProvider) : base(channel)
    {
        _serviceProvider = serviceProvider;
    }

    public override async Task HandleBasicDeliverAsync(
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = default
    )
    {
        var context = new MessageContext
        {
            DeliveryTag = deliveryTag,
            ConsumerTag = consumerTag,
            Exchange = exchange,
            RoutingKey = routingKey,
            IsRedelivered = redelivered,
            Properties = properties,
            Body = body
        };

        await using var scope = _serviceProvider.CreateAsyncScope();

        var consumerProvider = scope.ServiceProvider.GetRequiredService<IConsumerProvider>();
        if (!consumerProvider.TryGetConsumerTypeForMessage(context, out var consumeType))
        {
            await Channel.BasicRejectAsync(deliveryTag, false, cancellationToken);
            return;
        }

        var consumer = (IMessageConsumer) scope.ServiceProvider.GetRequiredService(consumeType);

        await consumer.ConsumeMessageAsync(context, cancellationToken);


        // var deserializer = scope.ServiceProvider.GetRequiredService<IMessageDeserializer>();
        // var message = deserializer.DeserializeMessage<IMessage>(body.Span);
    }
}

public record MessageContext
{
    public required ulong DeliveryTag { get; init; }
    public required string ConsumerTag { get; init; }
    public required string Exchange { get; init; }
    public required string RoutingKey { get; init; }
    public bool IsRedelivered { get; init; }
    public required IReadOnlyBasicProperties Properties { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
}

public interface IMessageConsumer
{
    Task ConsumeMessageAsync(MessageContext messageContext, CancellationToken cancellationToken = default);
}

public interface IConsumerProvider
{
    bool TryGetConsumerTypeForMessage(MessageContext messageContext, out Type consumerType);
}

public interface IMessageDeserializer
{
    TMessage DeserializeMessage<TMessage>(ReadOnlySpan<byte> message, Dictionary<string, object?>? headers = null);
}

public interface IMessageSerializer
{
    byte[] SerializeMessage<TMessage>(TMessage message);
}

public interface IMessagePublisher { }
