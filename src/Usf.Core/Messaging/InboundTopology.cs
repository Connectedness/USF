using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class InboundTopology : Topology<InboundEndpoint>, IInboundTopology
{
    private readonly IReadOnlyDictionary<InboundEndpointSelectionKey, InboundEndpoint> _dispatchIndex;

    public InboundTopology(
        IDictionary<string, InboundEndpoint> endpointsByName,
        IDictionary<InboundEndpointSelectionKey, InboundEndpoint>? dispatchIndex = null
    ) : base(endpointsByName)
    {
        _dispatchIndex = new ReadOnlyDictionary<InboundEndpointSelectionKey, InboundEndpoint>(
            new Dictionary<InboundEndpointSelectionKey, InboundEndpoint>(
                dispatchIndex ?? new Dictionary<InboundEndpointSelectionKey, InboundEndpoint>(),
                InboundEndpointSelectionKeyComparer.Instance
            )
        );
    }

    public IReadOnlyCollection<InboundEndpoint> Endpoints => Entries;

    public InboundEndpoint GetRequiredEndpoint(string name)
    {
        try
        {
            return GetRequired(name);
        }
        catch (TopologyEntryNotFoundException exception)
        {
            throw new InboundEndpointNotFoundException(name, exception);
        }
    }

    public bool TryGetEndpoint(string name, out InboundEndpoint? endpoint)
    {
        return TryGet(name, out endpoint);
    }

    internal bool TryDispatch(string source, string discriminator, out InboundEndpoint? endpoint)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(source));
        }

        if (string.IsNullOrWhiteSpace(discriminator))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(discriminator));
        }

        return _dispatchIndex.TryGetValue(new InboundEndpointSelectionKey(source, discriminator), out endpoint);
    }
}
