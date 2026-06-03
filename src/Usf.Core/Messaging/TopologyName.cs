using System;

namespace Usf.Core.Messaging;

public readonly record struct TopologyName
{
    private readonly string? _value;

    public TopologyName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(value));
        }

        _value = value;
    }

    public static TopologyName Default { get; } = new ("default");

    public string Value =>
        _value ?? throw new InvalidOperationException("A topology name must be initialized with a non-empty value.");

    public static implicit operator TopologyName(string value)
    {
        return new TopologyName(value);
    }

    public override string ToString()
    {
        return Value;
    }
}
