using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Usf.Core.Messaging;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class TopologyRuntimeHostedServiceTests
{
    [Fact]
    public async Task StartAsync_StartsEveryRuntime_AndStopsInReverseOrder()
    {
        var events = new List<string>();
        var first = new RecordingTopologyRuntime("first", events);
        var second = new RecordingTopologyRuntime("second", events);
        var hostedService = new TopologyRuntimeHostedService([first, second]);

        await hostedService.StartAsync(CancellationToken.None);
        await hostedService.StopAsync(CancellationToken.None);

        events.Should().Equal("start:first", "start:second", "stop:second", "stop:first");
    }

    [Fact]
    public void AddUsf_RegistersProvisioningHostedServiceBeforeRuntimeHostedService()
    {
        var services = new ServiceCollection();
        services.AddUsf();

        var hostedServiceImplementations = services
           .Where(static descriptor => descriptor.ServiceType == typeof(IHostedService))
           .Select(static descriptor => descriptor.ImplementationType)
           .ToList();

        var provisioningIndex = hostedServiceImplementations.IndexOf(typeof(TopologyProvisioningHostedService));
        var runtimeIndex = hostedServiceImplementations.IndexOf(typeof(TopologyRuntimeHostedService));

        provisioningIndex.Should().BeGreaterThanOrEqualTo(0);
        runtimeIndex.Should().BeGreaterThan(provisioningIndex);
    }

    private sealed class RecordingTopologyRuntime : ITopologyRuntime
    {
        private readonly List<string> _events;
        private readonly string _name;

        public RecordingTopologyRuntime(string name, List<string> events)
        {
            _name = name;
            _events = events;
            TopologyName = name;
        }

        public string TopologyName { get; }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            _events.Add($"start:{_name}");
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken = default)
        {
            _events.Add($"stop:{_name}");
            return Task.CompletedTask;
        }
    }
}
