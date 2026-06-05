using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Usf.Core.Messaging;

public static class OutboundDiagnostics
{
    public const string ActivitySourceName = "Usf.Outbound";

    public const string MessageTypeTagName = "usf.outbound.message.type";

    public const string TargetNameTagName = "usf.outbound.target.name";

    public const string TransportNameTagName = "usf.outbound.transport.name";

    public const string OutcomeTagName = "usf.outbound.outcome";

    public const string DeliveryFailureReasonTagName = "usf.outbound.delivery.failure.reason";

    public static readonly ActivitySource ActivitySource = new (ActivitySourceName);

    public static readonly Meter Meter = new (ActivitySourceName);

    public static readonly Counter<long> PublishAttempts = Meter.CreateCounter<long>("usf.outbound.publish.attempts");

    public static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>("usf.outbound.publish.failures");

    public static readonly Histogram<double> PublishDuration =
        Meter.CreateHistogram<double>("usf.outbound.publish.duration", unit: "ms");

    public static readonly Counter<long> TopologyProvisioningAttempts =
        Meter.CreateCounter<long>("usf.outbound.topology.provisioning.attempts");

    public static readonly Counter<long> TopologyProvisioningFailures =
        Meter.CreateCounter<long>("usf.outbound.topology.provisioning.failures");

    public static readonly Histogram<double> TopologyProvisioningDuration = Meter.CreateHistogram<double>(
        "usf.outbound.topology.provisioning.duration",
        unit: "ms"
    );
}
