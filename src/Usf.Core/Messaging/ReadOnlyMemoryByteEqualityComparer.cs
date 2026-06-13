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
        // We calculate the hash code with only the first 16 bytes and the last byte for memory instances
        // that are longer than 16 bytes. This lets us achieve O(1) performance instead of
        // O(n) for longer spans. Adding the length at the beginning is a reasonable way to avoid collisions.
        var span = value.Span;
        HashCode hashCode = new ();
        hashCode.Add(span.Length);

        var prefixLength = Math.Min(span.Length, 16);

        for (var index = 0; index < prefixLength; index++)
        {
            hashCode.Add(span[index]);
        }

        if (span.Length > prefixLength)
        {
            hashCode.Add(span[^1]);
        }

        return hashCode.ToHashCode();
    }
}
