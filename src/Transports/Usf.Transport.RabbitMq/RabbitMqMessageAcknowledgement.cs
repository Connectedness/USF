using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqMessageAcknowledgement : IMessageAcknowledgement
{
    private readonly IChannel _channel;
    private readonly ulong _deliveryTag;
    private int _settled;

    public RabbitMqMessageAcknowledgement(IChannel channel, ulong deliveryTag)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _deliveryTag = deliveryTag;
    }

    public Task AckAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return Task.CompletedTask;
        }

        return _channel.BasicAckAsync(_deliveryTag, multiple: false, cancellationToken).AsTask();
    }

    public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return Task.CompletedTask;
        }

        return _channel.BasicNackAsync(_deliveryTag, multiple: false, requeue, cancellationToken).AsTask();
    }
}
