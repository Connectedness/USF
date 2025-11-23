using System;
using System.Collections.Generic;
using System.Reflection;
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

        var consumerProvider = scope.ServiceProvider.GetRequiredService<IMessageHandlerTypeProvider>();

        if (!consumerProvider.TryGetHandlerTypeForMessage(context, out var handlerContext)
        {
            await Channel.BasicRejectAsync(deliveryTag, false, cancellationToken);
            return;
        }

        switch (messageHandlerType)
        {
            case MessageHandlerType.Request:
                var methodInfo = typeof(MessageReceiver).GetMethod(
                    nameof(HandleRequestAsync),
                    BindingFlags.Instance | BindingFlags.NonPublic
                )!;
                await (Task) methodInfo
                   .MakeGenericMethod(dtoType)
                   .Invoke(this, [context, scope, cancellationToken]);
                break;
            case MessageHandlerType.Notification:
                throw new NotImplementedException();
        }
    }

    private async Task HandleRequestAsync<TDto>(
        MessageContext messageContext,
        AsyncServiceScope scope,
        CancellationToken cancellationToken
    )
    {
        var serializer = scope.ServiceProvider.GetRequiredService<IMessageDeserializer>();
        var dto = serializer.DeserializeMessage<TDto>(messageContext.Body.Span);
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
        try
        {
            var response = await mediator.Send(dto, cancellationToken);
            // TODO: do something useful with the response -> e.g. publish a response message
            await Channel.BasicAckAsync(messageContext.DeliveryTag, false, cancellationToken);
        }
        catch (Exception)
        {
            // TODO: log error
            await Channel.BasicNackAsync(messageContext.DeliveryTag, false, true, cancellationToken);
        }
    }
}

public record MessageResponse { }

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

public interface IMessageHandlerTypeProvider
{
    bool TryGetHandlerTypeForMessage(
        MessageContext messageContext,
        out Type consumerType,
        out MessageHandlerType messageHandlerType
    );
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
