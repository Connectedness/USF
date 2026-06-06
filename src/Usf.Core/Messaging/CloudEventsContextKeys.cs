namespace Usf.Core.Messaging;

public static class CloudEventsContextKeys
{
    public static MessageContextKey<CloudEventEnvelope> Envelope { get; } = new ("cloudevents.envelope");
}
