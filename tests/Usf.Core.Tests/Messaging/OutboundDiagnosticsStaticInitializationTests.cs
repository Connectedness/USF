using System;
using System.Diagnostics;
using FluentAssertions;
using Usf.Core.Messaging;
using Xunit;

namespace Usf.Core.Tests.Messaging;

/// <summary>
/// Documents the static-initialization recursion that motivated exposing
/// <see cref="OutboundDiagnostics.ActivitySourceName" /> as a public const.
/// The bug is NOT a thread-safety problem (inline static field initializers are
/// thread-safe). It is single-threaded *re-entrancy*: the CLR type-init lock is
/// re-entrant for the thread that already holds it, so if a type's initializer
/// recurses back into the same type it observes whatever has been assigned so far
/// — which for a not-yet-assigned reference field is <c>null</c>.
/// Because the real <see cref="OutboundDiagnostics" /> is already fixed and type
/// initialization is process-global and one-shot, these tests reproduce the
/// mechanism on dedicated, self-contained types so the behaviour is deterministic
/// regardless of test ordering.
/// </summary>
[Collection("Diagnostics")]
public sealed class OutboundDiagnosticsStaticInitializationTests
{
    [Fact]
    public void DereferencingStaticActivitySourceFromListener_PoisonsTheTypeDuringInitialization()
    {
        // A listener whose predicate re-enters the diagnostics type. ActivitySource's
        // constructor invokes ShouldListenTo on every registered listener while a source
        // is being built, on the SAME thread, so the first thread to initialize
        // RecursiveDiagnostics re-enters its own .cctor and reads the still-null field.
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RecursiveDiagnostics.ActivitySource.Name,
            Sample = static (ref _) => ActivitySamplingResult.AllData
        };

        Exception? captured = null;
        try
        {
            // Either of these can be the first touch of RecursiveDiagnostics depending on
            // whether other ActivitySources already exist in the process: AddActivityListener
            // evaluates the predicate against existing sources, and the explicit access is a
            // fallback when this test runs in isolation. Whichever triggers the .cctor, the
            // recursive null dereference fails it and surfaces as TypeInitializationException.
            ActivitySource.AddActivityListener(listener);
            _ = RecursiveDiagnostics.ActivitySource;
        }
        catch (Exception ex)
        {
            captured = ex;
        }
        finally
        {
            // Unregister immediately: once the type is poisoned the predicate throws for
            // every source constructed anywhere in the process, so a lingering registration
            // would contaminate unrelated tests.
            listener.Dispose();
        }

        captured
           .Should().BeOfType<TypeInitializationException>()
           .Which.InnerException.Should().BeOfType<NullReferenceException>();
    }

    [Fact]
    public void ComparingAgainstConstName_DoesNotReEnterStaticInitialization()
    {
        using var listener = new ActivityListener();
        listener.ShouldListenTo = source => source.Name == ConstNameDiagnostics.ActivitySourceName;
        listener.Sample = static (ref _) => ActivitySamplingResult.AllData;
        ActivitySource.AddActivityListener(listener);

        var triggerInitialization = static () => _ = ConstNameDiagnostics.ActivitySource;

        triggerInitialization.Should().NotThrow();
        ConstNameDiagnostics.ActivitySource.Name.Should().Be(ConstNameDiagnostics.ActivitySourceName);
    }

    [Fact]
    public void OutboundDiagnostics_ActivitySourceNameMatchesActivitySourceAndMeter()
    {
        OutboundDiagnostics.ActivitySourceName.Should().Be("Usf.Outbound");
        OutboundDiagnostics.ActivitySource.Name.Should().Be(OutboundDiagnostics.ActivitySourceName);
        OutboundDiagnostics.Meter.Name.Should().Be(OutboundDiagnostics.ActivitySourceName);
    }

    /// <summary>
    /// Mirrors the ORIGINAL <see cref="OutboundDiagnostics" /> shape: the only way to
    /// learn the source name is to dereference the static <see cref="ActivitySource" />.
    /// </summary>
    private static class RecursiveDiagnostics
    {
        public static readonly ActivitySource ActivitySource = new ("Usf.Tests.RecursiveDiagnostics");
    }

    /// <summary>
    /// Mirrors the FIX: the name is a compile-time const, so a listener predicate that
    /// compares against it is inlined and never touches the diagnostics type.
    /// </summary>
    private static class ConstNameDiagnostics
    {
        public const string ActivitySourceName = "Usf.Tests.ConstNameDiagnostics";

        public static readonly ActivitySource ActivitySource = new (ActivitySourceName);
    }
}
