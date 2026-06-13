using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Messaging;

[Collection("Diagnostics")]
public sealed class MessagePublisherTests
{
    private const string UninitializedTopologyPublisherMessage =
        "TopologyPublisher must not be the default instance";

    [Fact]
    public async Task PublishMessageAsync_UsesTopologyResolvedTarget_WhenNoExplicitTargetIsProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>("default", CloudEventsTestFactory.CreateSerializer());
        var topology = new OutboundTopology(
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
        );
        var publisher = new MessagePublisher(topology);
        var message = new SampleMessage("hello");

        await publisher.PublishMessageAsync(message, cancellationToken: cancellationToken);

        target.Messages.Should().ContainSingle().Which.Should().Be(message);
        var envelope = target.CloudEventEnvelopes.Should().ContainSingle().Which;
        Encoding.UTF8.GetString(envelope.Data).Should().Be("{\"Value\":\"hello\"}");
        envelope.Type.Should().Be(CloudEventsTestFactory.SampleDiscriminator);
        envelope.Source.Should().Be("/tests/core");
        envelope.DataContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task PublishMessageAsync_UsesExplicitTarget_WhenProvided()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var explicitTarget = new RecordingTarget<SampleMessage>("explicit", CloudEventsTestFactory.CreateSerializer());
        var publisher = new MessagePublisher(new EmptyOutboundTopology());
        var message = new SampleMessage("hello");

        await publisher.PublishMessageAsync(
            message,
            explicitTarget,
            "orders.created",
            cancellationToken
        );

        explicitTarget.Messages.Should().ContainSingle().Which.Should().Be(message);
        explicitTarget.RoutingKeys.Should().ContainSingle().Which.Should().Be("orders.created");
        Encoding.UTF8.GetString(explicitTarget.CloudEventEnvelopes.Should().ContainSingle().Which.Data)
           .Should()
           .Be("{\"Value\":\"hello\"}");
    }

    [Fact]
    public async Task PublishMessageAsync_ForwardsRoutingKeyToTopologyResolvedTarget()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>("default", CloudEventsTestFactory.CreateSerializer());
        var topology = new OutboundTopology(
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
        );
        var publisher = new MessagePublisher(topology);

        await publisher.PublishMessageAsync(
            new SampleMessage("hello"),
            routingKey: "tenant-a.created",
            cancellationToken: cancellationToken
        );

        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("tenant-a.created");
    }

    [Fact]
    public async Task TopologyPublisher_PublishMessageAsync_ForwardsRoutingKeyToExplicitTarget()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>(
            "legacy",
            CloudEventsTestFactory.CreateSerializer(),
            CloudEventsTestFactory.CreateRegistry(),
            "legacy"
        );
        var publisher = new MessagePublisher(new EmptyOutboundTopology());

        await publisher
           .ForTopology("legacy")
           .PublishMessageAsync(
                new SampleMessage("hello"),
                target,
                routingKey: "legacy.created",
                cancellationToken: cancellationToken
            );

        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("legacy.created");
    }

    [Fact]
    public async Task PublishMessageAsync_UsesExplicitMetadata_ForMessagesThatCannotImplementICloudEvent()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var registry = CloudEventsTestFactory.CreateRegistry(
            new KeyValuePair<Type, string>(typeof(ThirdPartyMessage), "tests.third-party")
        );
        var serializer = new CloudEventMessageSerializer(
            new Utf8JsonPayloadCodec(),
            new CloudEventsOptions { Source = "/tests/core" }
        );
        var target = new RecordingTarget<ThirdPartyMessage>("third-party", serializer, registry);
        var publisher = new MessagePublisher(new EmptyOutboundTopology());
        CloudEventMetadata metadata = new (
            Guid.Parse("f39b562b-b846-48e6-a693-4108015e7c82"),
            new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero),
            "subject"
        );

        await publisher.PublishMessageAsync(
            new ThirdPartyMessage("hello"),
            in metadata,
            target,
            routingKey: "third-party.created",
            cancellationToken: cancellationToken
        );

        var envelope = target.CloudEventEnvelopes.Should().ContainSingle().Which;
        envelope.Id.Should().Be("f39b562b-b846-48e6-a693-4108015e7c82");
        envelope.Subject.Should().Be("subject");
        envelope.Type.Should().Be("tests.third-party");
        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("third-party.created");
    }

    [Fact]
    public async Task PublishMessageAsync_RejectsNullMessages()
    {
        var publisher = new MessagePublisher(new EmptyOutboundTopology());
        var metadata = default(CloudEventMetadata);

        var action = async () => await publisher.PublishMessageAsync<string>(null!, in metadata);

        await action.Should().ThrowAsync<ArgumentNullException>().WithParameterName("message");
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsWhenNoTargetIsConfigured()
    {
        var publisher = new MessagePublisher(new EmptyOutboundTopology());

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"));

        await action.Should().ThrowAsync<OutboundTargetNotFoundException>();
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsFailFastErrorWhenSelectedTopologyIsNotRegistered()
    {
        var publisher = new MessagePublisher(new EmptyOutboundTopologyRegistry());

        var action = async () => await publisher
           .ForTopology("missing")
           .PublishMessageAsync(new SampleMessage("hello"));

        var exception = (await action.Should().ThrowAsync<InvalidOperationException>()).Which;
        exception.Message.Should().Be(
            "Outbound topology 'missing' is not registered. Registered outbound topologies: (none)."
        );
    }

    [Fact]
    public async Task TopologyPublisher_PublishMessageAsync_ThrowsClearErrorWhenDefaultConstructed()
    {
        TopologyPublisher publisher = default;

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"));

        var exception = (await action.Should().ThrowAsync<InvalidOperationException>()).Which;
        exception.Message.Should().Be(UninitializedTopologyPublisherMessage);
    }

    [Fact]
    public async Task TopologyPublisher_PublishMessageWithMetadataAsync_ThrowsClearErrorWhenDefaultConstructed()
    {
        TopologyPublisher publisher = default;
        CloudEventMetadata metadata = new (
            Guid.Parse("e3fe171f-4684-40db-956b-ff03476f6e03"),
            new DateTimeOffset(2026, 6, 4, 12, 0, 0, TimeSpan.Zero)
        );

        var action = async () => await publisher.PublishMessageAsync(
            new ThirdPartyMessage("hello"),
            in metadata
        );

        var exception = (await action.Should().ThrowAsync<InvalidOperationException>()).Which;
        exception.Message.Should().Be(UninitializedTopologyPublisherMessage);
    }

    [Fact]
    public async Task TopologyPublisher_PublishRawAsync_ThrowsClearErrorWhenDefaultConstructed()
    {
        TopologyPublisher publisher = default;
        SerializedMessage message = new (
            "prepared"u8.ToArray(),
            "application/custom",
            "utf-8",
            new Dictionary<string, string?>(StringComparer.Ordinal),
            null,
            null
        );
        var target = new RecordingTarget<SampleMessage>("raw", CloudEventsTestFactory.CreateSerializer());

        var action = async () => await publisher.PublishRawAsync(message, target);

        var exception = (await action.Should().ThrowAsync<InvalidOperationException>()).Which;
        exception.Message.Should().Be(UninitializedTopologyPublisherMessage);
    }

    [Fact]
    public async Task PublishMessageAsync_RejectsExplicitTargetWhenNonDefaultTopologyDisagrees()
    {
        var target = new RecordingTarget<SampleMessage>(
            "explicit",
            CloudEventsTestFactory.CreateSerializer(),
            CloudEventsTestFactory.CreateRegistry(),
            "legacy"
        );
        var publisher = new MessagePublisher(new EmptyOutboundTopology());

        var action = async () => await publisher
           .ForTopology("modern")
           .PublishMessageAsync(new SampleMessage("hello"), target);

        var exception = (await action.Should().ThrowAsync<InvalidOperationException>()).Which;
        exception.Message.Should().Be(
            "Outbound target 'explicit' belongs to outbound topology 'legacy', but publish requested outbound topology 'modern'."
        );
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsWhenExplicitTargetDoesNotMatchMessageType()
    {
        var target = new RecordingTarget<OtherMessage>("other", CloudEventsTestFactory.CreateSerializer());
        var publisher = new MessagePublisher(new EmptyOutboundTopology());

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"), target);

        await action.Should().ThrowAsync<OutboundTargetTypeMismatchException>();
    }

    [Fact]
    public async Task PublishMessageAsync_ResolvesContractsByRuntimeTypeForSubtypes()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        MessageContractRegistryBuilder registryBuilder = new ();
        registryBuilder.Map<BaseSampleMessage>("tests.base");
        registryBuilder.Map<DerivedSampleMessage>("tests.derived").WithDataSchema("/schemas/derived");
        var registry = registryBuilder.Build();
        var target = new RecordingTarget<BaseSampleMessage>(
            "base",
            CloudEventsTestFactory.CreateSerializer(),
            registry
        );
        var publisher = new MessagePublisher(new EmptyOutboundTopology());
        BaseSampleMessage message = new DerivedSampleMessage("hello", "detail");

        await publisher.PublishMessageAsync(message, target, cancellationToken: cancellationToken);

        var envelope = target.CloudEventEnvelopes.Should().ContainSingle().Which;
        envelope.Type.Should().Be("tests.derived");
        envelope.DataSchema.Should().Be("/schemas/derived");
    }

    [Fact]
    public async Task PublishRawAsync_PublishesSerializedMessageWithoutInvokingSerializer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>(
            "raw",
            new ThrowingSerializer(new InvalidOperationException("serializer should not run"))
        );
        var publisher = new MessagePublisher(new EmptyOutboundTopology());
        SerializedMessage message = new (
            "prepared"u8.ToArray(),
            "application/custom",
            "utf-8",
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["tenant"] = "a"
            },
            "message-id",
            "correlation-id"
        );

        await publisher.PublishRawAsync(message, target, cancellationToken);

        target.Messages.Should().BeEmpty();
        target.RoutingKeys.Should().BeEmpty();
        target.SerializedMessages.Should().ContainSingle().Which.Should().Be(message);
    }

    [Fact]
    public async Task PublishRawAsync_RejectsMessagesWithoutABody()
    {
        var target = new RecordingTarget<SampleMessage>("raw", CloudEventsTestFactory.CreateSerializer());
        var publisher = new MessagePublisher(new EmptyOutboundTopology());

        var action = async () => await publisher.PublishRawAsync(default, target);

        await action.Should().ThrowAsync<ArgumentException>().WithParameterName("message");
        target.SerializedMessages.Should().BeEmpty();
    }

    [Fact]
    public async Task PublishMessageAsync_TagsDeliveryFailureReason()
    {
        var measurements = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Name == "usf.outbound.publish.failures")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) => measurements.Add(tags.ToArray()));
        listener.Start();

        var deliveryException = new MessageDeliveryException(
            "target",
            MessageDeliveryFailureReason.Returned,
            new InvalidOperationException("returned")
        );
        var target = new ThrowingTarget<SampleMessage>(
            "target",
            CloudEventsTestFactory.CreateSerializer(),
            deliveryException
        );
        var publisher = new MessagePublisher(new EmptyOutboundTopology());

        var action = async () => await publisher.PublishMessageAsync(new SampleMessage("hello"), target);

        await action.Should().ThrowAsync<MessageDeliveryException>();
        measurements.Should().ContainSingle();
        measurements[0].Should().Contain(
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, "failure")
        );
        measurements[0].Should().Contain(
            new KeyValuePair<string, object?>(OutboundDiagnostics.DeliveryFailureReasonTagName, "returned")
        );
        measurements[0].Should().Contain(
            new KeyValuePair<string, object?>(
                OutboundDiagnostics.MessageTypeTagName,
                CloudEventsTestFactory.SampleDiscriminator
            )
        );
    }
}
