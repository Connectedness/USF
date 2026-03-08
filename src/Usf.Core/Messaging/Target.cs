using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public abstract class Target
{
    protected Target(Type messageType, string name, string transportName)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (string.IsNullOrWhiteSpace(transportName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(transportName));
        }

        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        Name = name;
        TransportName = transportName;
    }

    public Type MessageType { get; }

    public string Name { get; }

    public string TransportName { get; }

    public abstract Task PublishUntypedAsync(object message, CancellationToken cancellationToken = default);
}
