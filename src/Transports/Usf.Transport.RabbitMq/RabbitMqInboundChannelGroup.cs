using System;
using RabbitMQ.Client;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundChannelGroup
{
    public RabbitMqInboundChannelGroup(
        string name,
        int maximumChannelCount,
        ushort prefetchCount,
        ushort consumerDispatchConcurrency
    )
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (maximumChannelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumChannelCount),
                maximumChannelCount,
                "The value must be greater than zero."
            );
        }

        if (prefetchCount == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prefetchCount),
                prefetchCount,
                "The value must be greater than zero."
            );
        }

        if (consumerDispatchConcurrency == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerDispatchConcurrency),
                consumerDispatchConcurrency,
                "The value must be greater than zero."
            );
        }

        Name = name;
        MaximumChannelCount = maximumChannelCount;
        PrefetchCount = prefetchCount;
        ConsumerDispatchConcurrency = consumerDispatchConcurrency;
    }

    public string Name { get; }

    public int MaximumChannelCount { get; }

    public ushort PrefetchCount { get; }

    public ushort ConsumerDispatchConcurrency { get; }

    public CreateChannelOptions CreateChannelOptions()
    {
        return new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            outstandingPublisherConfirmationsRateLimiter: null,
            consumerDispatchConcurrency: ConsumerDispatchConcurrency
        );
    }
}
