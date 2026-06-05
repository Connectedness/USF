using Xunit;

namespace Usf.Core.Tests.Messaging;

/// <summary>
/// Groups tests that mutate process-global diagnostics registries
/// (<see cref="System.Diagnostics.ActivityListener" />,
/// <see cref="System.Diagnostics.Metrics.MeterListener" />) so they never run in
/// parallel with one another. A registered ActivityListener predicate or a MeterListener
/// measurement callback fires for sources/instruments created anywhere in the process, so
/// concurrent execution could cross-contaminate captured measurements or invoke a
/// predicate at an unexpected time.
/// This is plain serialization, not shared state, so there is intentionally no
/// <c>ICollectionFixture&lt;T&gt;</c> — there is nothing to share. Tests that only touch
/// the flow-scoped <see cref="System.Diagnostics.Activity.Current" /> (an AsyncLocal) do
/// not need to join, because each test runs in its own execution context.
/// </summary>
[CollectionDefinition("Diagnostics", DisableParallelization = true)]
public sealed class DiagnosticsCollection;
