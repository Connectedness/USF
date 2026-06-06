using System.Collections.Generic;

namespace Usf.Core.Messaging;

public interface IInboundTopology
{
    IReadOnlyCollection<InboundEndpoint> Endpoints { get; }

    InboundEndpoint GetRequiredEndpoint(string name);

    bool TryGetEndpoint(string name, out InboundEndpoint? endpoint);
}
