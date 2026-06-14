using System;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class MessageContractRegistryTests
{
    [Fact]
    public void Build_MapsCanonicalDiscriminatorAndInboundAliasesAsymmetrically()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.current").WithInboundAlias("registry.legacy");

        var registry = builder.Build();

        registry.GetDiscriminator(typeof(RegistryMessage)).Should().Be("registry.current");
        registry.GetInboundDiscriminators(typeof(RegistryMessage)).Should().Equal(
            "registry.current",
            "registry.legacy"
        );
        registry.TryResolveType("registry.current", out var current).Should().BeTrue();
        current.Should().Be<RegistryMessage>();
        registry.TryResolveType("registry.legacy", out var legacy).Should().BeTrue();
        legacy.Should().Be<RegistryMessage>();
    }

    [Fact]
    public void Build_AllowsPublishOnlyMappingWithoutInboundRegistration()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.MapOutbound<RegistryMessage>("registry.outbound");

        var registry = builder.Build();

        registry.GetDiscriminator(typeof(RegistryMessage)).Should().Be("registry.outbound");
        registry.GetInboundDiscriminators(typeof(RegistryMessage)).Should().BeEmpty();
        registry.TryResolveType("registry.outbound", out _).Should().BeFalse();
    }

    [Fact]
    public void Build_ReportsAllConflictingEntries()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.shared");
        builder.Map<RegistryMessage>("registry.second");
        builder.Map<OtherRegistryMessage>("registry.shared");
        builder.MapOutbound<OutboundRegistryMessage>("registry.outbound").WithInboundAlias("registry.legacy");

        Action action = () => builder.Build();

        var exception = action.Should().Throw<MessageContractRegistryValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "CloudEvents discriminator 'registry.shared' maps to multiple message types: 'Usf.Core.Tests.Messaging.MessageContractRegistryTests+OtherRegistryMessage', 'Usf.Core.Tests.Messaging.MessageContractRegistryTests+RegistryMessage'.",
            "Message type 'Usf.Core.Tests.Messaging.MessageContractRegistryTests+OutboundRegistryMessage' registers inbound CloudEvents discriminators but does not accept its canonical discriminator 'registry.outbound' inbound.",
            "Message type 'Usf.Core.Tests.Messaging.MessageContractRegistryTests+RegistryMessage' has multiple canonical CloudEvents discriminators: 'registry.second', 'registry.shared'."
        );
    }

    [Fact]
    public void Build_ReportsDuplicateInboundEntryInsteadOfFailingWhileConstructingRegistry()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.current").WithInboundAlias("registry.current");

        Action action = () => builder.Build();

        var exception = action.Should().Throw<MessageContractRegistryValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle()
           .Which.Should()
           .Be(
                "Inbound CloudEvents discriminator 'registry.current' is registered multiple times for message type 'Usf.Core.Tests.Messaging.MessageContractRegistryTests+RegistryMessage'."
            );
    }

    private sealed record RegistryMessage;

    private sealed record OtherRegistryMessage;

    private sealed record OutboundRegistryMessage;
}
