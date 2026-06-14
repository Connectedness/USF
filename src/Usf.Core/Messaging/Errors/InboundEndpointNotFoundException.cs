using System;

namespace Usf.Core.Messaging.Errors;

public sealed class InboundEndpointNotFoundException : Exception
{
    public InboundEndpointNotFoundException(string endpointName)
        : base($"Inbound endpoint '{endpointName}' is not registered.")
    {
        EndpointName = endpointName;
    }

    public InboundEndpointNotFoundException(string endpointName, Exception innerException)
        : base($"Inbound endpoint '{endpointName}' is not registered.", innerException)
    {
        EndpointName = endpointName;
    }

    public string EndpointName { get; }
}
