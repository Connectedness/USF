## Relevant RabbitMQ.Client Classes/Interfaces

* `ConnectionFactory`: used to create a long-lived `IConnection` instance using a URI (e.g.
  `amqp://guest:guest@localhost:5672`)
* `IConnection`: represents a connection to the RabbitMQ server, long-lived, re-use
* `IChannel`: represents a channel to the RabbitMQ server for most of the operations, short-lived, re-use, create on
  demand
* `IAsyncBasicConsumer`: represents a consumer of messages from the RabbitMQ server. Can use `AsyncDefaultBasicConsumer`
  as a base class or `AsyncEventingBasicConsumer` as an entry without writing dedicated consumers

## RabbitMQ.Client Behavior

### Connection Recovery

RabbitMQ.Client provides built-in connection and topology recovery. No need to handle this manually.

```csharp
var factory = new ConnectionFactory
{
    AutomaticRecoveryEnabled = true
};
```

See [RabbitMQ.Client documentation](https://www.rabbitmq.com/client-libraries/dotnet-api-guide#recovery) for more
information.

## What We Need

Generally:

* Create and configure a `ConnectionFactory` instance
* Create an `IConnection` using the `ConnectionFactory`
* Create `IChannel` using the `IConnection` where needed

Publishing messages to RabbitMQ:

* A custom publisher service containing an `IChannel` for publishing to exchanges/queues
* Use a middleware pipeline to transform/enrich messages before publishing?
* The publisher should always use `publisher confirms` to ensure messages are delivered;
  see [Reliable Publishing with Publisher Confirms](https://www.rabbitmq.com/tutorials/tutorial-seven-dotnet)

Receiving messages from RabbitMQ:

* Use a (custom) consumer to listen for messages with the `IChannel`
* Use a middleware pipeline to transform/enrich messages before processing?
* The consumer must preprocess the message to determine its type and route it to the appropriate handler
* For each message a DI scope is created and the handler is resolved from the scope
* The handler is called to process the message
* The handler can reject or nack the message using exceptions? ❓
* The consumer actually acks, nacks, or rejects the message based on handler behavior. Processing the message without
  throwing an exception results in acknowledging the message


