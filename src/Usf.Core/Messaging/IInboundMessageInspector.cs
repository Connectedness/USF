using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IInboundMessageInspector
{
    ValueTask<InboundMessageInspectionResult> InspectAsync(
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    );
}
