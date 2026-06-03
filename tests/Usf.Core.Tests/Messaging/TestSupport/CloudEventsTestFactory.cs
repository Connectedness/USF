using System;
using System.Collections.Generic;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;

namespace Usf.Core.Tests.Messaging.TestSupport;

public static class CloudEventsTestFactory
{
    public const string SampleDiscriminator = "tests.sample-message";

    public static IMessageContractRegistry CreateRegistry()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<SampleMessage>(SampleDiscriminator).WithInboundAlias("tests.legacy-sample-message");
        return builder.Build();
    }

    public static CloudEventMessageSerializer CreateSerializer()
    {
        return new CloudEventMessageSerializer(
            new Utf8JsonPayloadCodec(),
            new CloudEventsOptions
            {
                Source = "/tests/core"
            }
        );
    }

    public static IMessageContractRegistry CreateRegistry(params KeyValuePair<Type, string>[] mappings)
    {
        Dictionary<Type, string> discriminators = new ();
        Dictionary<string, Type> inbound = new (StringComparer.Ordinal);

        foreach (var mapping in mappings)
        {
            discriminators.Add(mapping.Key, mapping.Value);
            inbound.Add(mapping.Value, mapping.Key);
        }

        return new MessageContractRegistry(discriminators, inbound, new Dictionary<Type, string>());
    }
}
