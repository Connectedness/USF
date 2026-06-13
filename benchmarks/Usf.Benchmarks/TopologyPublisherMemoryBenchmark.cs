using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Usf.Abstractions;
using Usf.Core.Messaging;

namespace Usf.Benchmarks;

[MemoryDiagnoser]
public class TopologyPublisherMemoryBenchmark
{
    private static readonly TopologyName NamedTopology = new ("benchmark");

    private BenchmarkMessage _message = null!;
    private MessagePublisher _publisher = null!;
    private BenchmarkTarget _target = null!;

    [GlobalSetup]
    public void Setup()
    {
        MessageContractRegistryBuilder contracts = new ();
        contracts.Map<BenchmarkMessage>("benchmarks.message");
        var registry = contracts.Build();
        _target = new BenchmarkTarget(registry, NamedTopology);
        _publisher = new MessagePublisher(
            new OutboundTopology(
                new Dictionary<Type, OutboundTarget>(),
                new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            )
        );
        _message = new BenchmarkMessage("value");
    }

    [Benchmark(Baseline = true)]
    public Task DefaultPublish()
    {
        return _publisher.PublishMessageAsync(_message, _target, cancellationToken: CancellationToken.None);
    }

    [Benchmark]
    public Task TopologyPublisherPublish()
    {
        return _publisher
           .ForTopology(NamedTopology)
           .PublishMessageAsync(_message, _target, cancellationToken: CancellationToken.None);
    }

    private sealed record BenchmarkMessage(string Value) : ICloudEvent
    {
        Guid ICloudEvent.Id { get; } = UsfUuid.NewId();

        DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

        string? ICloudEvent.Subject => null;
    }

    private sealed class BenchmarkTarget : OutboundTarget<BenchmarkMessage>
    {
        public BenchmarkTarget(IMessageContractRegistry messageContractRegistry, TopologyName topologyName)
            : base("benchmark", "benchmark", new BenchmarkSerializer(), messageContractRegistry, topologyName) { }

        public override Task PublishSerializedAsync(
            SerializedMessage message,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }

        protected override Task PublishTypedCloudEventAsync(
            BenchmarkMessage message,
            CloudEventEnvelope envelope,
            string? routingKey,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class BenchmarkSerializer : IMessageSerializer
    {
        private static readonly byte[] Body = "body"u8.ToArray();

        public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
            T message,
            in CloudEventMetadata metadata,
            string? type,
            string? dataSchema,
            CancellationToken cancellationToken = default
        )
        {
            CloudEventEnvelope envelope = new (
                "1.0",
                metadata.Id.ToString("D"),
                "/benchmarks",
                type!,
                metadata.Time,
                metadata.Subject,
                "application/json",
                dataSchema,
                Body
            );
            return new ValueTask<CloudEventEnvelope>(envelope);
        }
    }
}
