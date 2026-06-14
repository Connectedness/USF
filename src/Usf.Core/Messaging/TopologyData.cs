using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Usf.Core.Messaging;

public readonly record struct TopologyData(
    FrozenDictionary<Type, OutboundTarget> TargetsByMessageType,
    FrozenDictionary<string, OutboundTarget> TargetsByName,
    FrozenDictionary<string, InboundEndpoint> EndpointsByName,
    ImmutableArray<OutboundTarget> OutboundTargets,
    ImmutableArray<InboundEndpoint> InboundEndpoints
)
{
    public static TopologyData PrepareTopologyDataStructures(
        IDictionary<Type, OutboundTarget> targetsByMessageType,
        IDictionary<string, OutboundTarget> targetsByName,
        IDictionary<string, InboundEndpoint> endpointsByName
    )
    {
        if (targetsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(targetsByMessageType));
        }

        if (targetsByName is null)
        {
            throw new ArgumentNullException(nameof(targetsByName));
        }

        if (endpointsByName is null)
        {
            throw new ArgumentNullException(nameof(endpointsByName));
        }

        var frozenTargetsByMessageType = targetsByMessageType.ToFrozenDictionary();
        var frozenTargetsByName = targetsByName.ToFrozenDictionary(StringComparer.Ordinal);
        var frozenEndpointsByName = endpointsByName.ToFrozenDictionary(StringComparer.Ordinal);

        return new TopologyData(
            frozenTargetsByMessageType,
            frozenTargetsByName,
            frozenEndpointsByName,
            [..frozenTargetsByMessageType.Values.Concat(frozenTargetsByName.Values).Distinct()],
            [..frozenEndpointsByName.Values]
        );
    }
}
