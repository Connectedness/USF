namespace Usf.Transport.RabbitMq.Configuration;

public sealed record RabbitMqInboundChannelGroupDefinition(
    string Name,
    int MaximumChannelCount,
    ushort PrefetchCount,
    ushort ConsumerDispatchConcurrency
)
{
    public const string ReservedImplicitNamePrefix = "$implicit:";
}
