# USF RabbitMQ Transport

## Automatic Recovery

USF requires RabbitMQ.Client automatic connection recovery. Keep
`ConnectionFactory.AutomaticRecoveryEnabled` set to `true`. RabbitMQ.Client owns
reconnection attempts and USF continues to use the same autorecovering
connection wrapper throughout the outbound topology lifetime.

`ConnectionFactory.NetworkRecoveryInterval` remains caller-configurable.
`ConnectionFactory.TopologyRecoveryEnabled` also remains caller-controlled. It
defaults to `true` in RabbitMQ.Client and governs recovery of exchanges, queues,
and bindings rather than connection recovery. It can be disabled when broker
topology is provisioned externally.

Automatic recovery is an availability mechanism, not a delivery guarantee. It
does not buffer or republish failed or in-flight messages during an outage.
Applications that require at-least-once effects must retry safely or use an
outbox.

## Cluster Channel Limit

RabbitMQ cluster nodes should use a consistent `channel_max`. USF validates its
worst-case outbound channel budget against the broker limit negotiated on the
initial connection. RabbitMQ.Client performs subsequent connection recovery
internally.
