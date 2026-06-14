using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public sealed class InboundEndpointSelectionKeyComparer : IEqualityComparer<InboundEndpointSelectionKey>
{
    public static InboundEndpointSelectionKeyComparer Instance { get; } = new ();

    public bool Equals(InboundEndpointSelectionKey x, InboundEndpointSelectionKey y)
    {
        return string.Equals(x.Source, y.Source, StringComparison.Ordinal) &&
               string.Equals(x.Discriminator, y.Discriminator, StringComparison.Ordinal);
    }

    public int GetHashCode(InboundEndpointSelectionKey obj) => HashCode.Combine(obj.Source, obj.Discriminator);
}
