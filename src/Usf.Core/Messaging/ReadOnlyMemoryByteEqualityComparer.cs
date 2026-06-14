using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public sealed class ReadOnlyMemoryByteEqualityComparer : IEqualityComparer<ReadOnlyMemory<byte>>
{
    private ReadOnlyMemoryByteEqualityComparer() { }

    public static ReadOnlyMemoryByteEqualityComparer Default { get; } = new ();

    public bool Equals(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return x.Span.SequenceEqual(y.Span);
    }

    public int GetHashCode(ReadOnlyMemory<byte> value)
    {
        // We calculate the hash code with only the length and the last byte. This lets us achieve O(1) performance
        // instead of O(n) for longer spans.
        var span = value.Span;
        return span.IsEmpty ? 0 : HashCode.Combine(span.Length, span[^1]);
    }
}
