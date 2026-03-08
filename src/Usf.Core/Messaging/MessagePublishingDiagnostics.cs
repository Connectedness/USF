using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Usf.Core.Messaging;

public static class MessagePublishingDiagnostics
{
    public const string MessageTypeTagName = "message.type";

    public const string TargetNameTagName = "target.name";

    public const string TransportNameTagName = "transport.name";

    public const string OutcomeTagName = "outcome";

    public static readonly ActivitySource ActivitySource = new ("Usf.Messaging");

    public static readonly Meter Meter = new ("Usf.Messaging");

    public static readonly Counter<long> PublishAttempts = Meter.CreateCounter<long>("usf.messaging.publish.attempts");

    public static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>("usf.messaging.publish.failures");

    public static readonly Histogram<double> PublishDuration =
        Meter.CreateHistogram<double>("usf.messaging.publish.duration", unit: "ms");

    public static readonly Counter<long> TopologyProvisioningAttempts =
        Meter.CreateCounter<long>("usf.messaging.topology.provisioning.attempts");

    public static readonly Counter<long> TopologyProvisioningFailures =
        Meter.CreateCounter<long>("usf.messaging.topology.provisioning.failures");

    public static readonly Histogram<double> TopologyProvisioningDuration = Meter.CreateHistogram<double>(
        "usf.messaging.topology.provisioning.duration",
        unit: "ms"
    );
}
