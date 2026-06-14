using System;
using System.Collections;
using System.Collections.Generic;
using FluentAssertions;
using RabbitMQ.Client;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqTransportMessageTests
{
    [Fact]
    public void Constructor_CopiesBodyByDefault()
    {
        var body = new byte[] { 1, 2, 3 };

        var message = CreateMessage(body);
        body[0] = 9;

        message.Body.Span.ToArray().Should().Equal(1, 2, 3);
    }

    [Fact]
    public void Constructor_AliasesBodyWhenCopyingIsDisabled()
    {
        var bodyBytes = new byte[] { 0, 1, 2, 3 };
        ReadOnlyMemory<byte> body = bodyBytes.AsMemory(1, 2);

        var message = CreateMessage(body, copyBody: false);
        bodyBytes[1] = 9;

        message.Body.Span.ToArray().Should().Equal(9, 2);
    }

    [Fact]
    public void Constructor_ReusesReadOnlyHeadersAndUsesDeliveryCount()
    {
        var headers = new Dictionary<string, object?>
        {
            ["x-delivery-count"] = 3L,
            ["custom"] = "original"
        };

        var message = CreateMessage(ReadOnlyMemory<byte>.Empty, headers);
        headers["custom"] = "changed";

        message.DeliveryAttempt.Should().Be(4);
        message.Headers.Should().BeSameAs(headers);
        message.Headers["custom"].Should().Be("changed");
    }

    [Fact]
    public void Constructor_WrapsHeadersWithoutCopyingWhenDictionaryIsNotReadOnlyDictionary()
    {
        var headers = new CountingDictionary
        {
            ["custom"] = "original"
        };

        var message = CreateMessage(ReadOnlyMemory<byte>.Empty, headers);
        headers["custom"] = "changed";

        headers.EnumerationCount.Should().Be(0);
        message.Headers.Should().NotBeSameAs(headers);
        message.Headers["custom"].Should().Be("changed");
    }

    [Fact]
    public void Constructor_UsesDeathCountWhenDeliveryCountIsAbsent()
    {
        var headers = new Dictionary<string, object?>
        {
            ["x-death"] = new List<object?>
            {
                new Dictionary<string, object?> { ["count"] = 5L }
            }
        };

        var message = CreateMessage(ReadOnlyMemory<byte>.Empty, headers);

        message.DeliveryAttempt.Should().Be(6);
    }

    [Fact]
    public void Constructor_UsesRedeliveredFallbackWhenAttemptHeadersAreAbsent()
    {
        var message = CreateMessage(ReadOnlyMemory<byte>.Empty, redelivered: true);

        message.DeliveryAttempt.Should().Be(2);
    }

    private static RabbitMqTransportMessage CreateMessage(
        ReadOnlyMemory<byte> body,
        IDictionary<string, object?>? headers = null,
        bool copyBody = true,
        bool redelivered = false
    )
    {
        BasicProperties basicProperties = new ()
        {
            Headers = headers
        };

        return new RabbitMqTransportMessage(
            "queue",
            "consumer",
            42,
            redelivered,
            "exchange",
            "routing-key",
            basicProperties,
            body,
            copyBody
        );
    }

    private sealed class CountingDictionary : IDictionary<string, object?>
    {
        private readonly Dictionary<string, object?> _values = new (StringComparer.Ordinal);

        public int EnumerationCount { get; private set; }

        public object? this[string key]
        {
            get => _values[key];
            set => _values[key] = value;
        }

        public ICollection<string> Keys => _values.Keys;

        public ICollection<object?> Values => _values.Values;

        public int Count => _values.Count;

        public bool IsReadOnly => false;

        public void Add(string key, object? value)
        {
            _values.Add(key, value);
        }

        public void Add(KeyValuePair<string, object?> item)
        {
            ((ICollection<KeyValuePair<string, object?>>) _values).Add(item);
        }

        public void Clear()
        {
            _values.Clear();
        }

        public bool Contains(KeyValuePair<string, object?> item)
        {
            return ((ICollection<KeyValuePair<string, object?>>) _values).Contains(item);
        }

        public bool ContainsKey(string key)
        {
            return _values.ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<string, object?>[] array, int arrayIndex)
        {
            ((ICollection<KeyValuePair<string, object?>>) _values).CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
        {
            EnumerationCount++;
            return _values.GetEnumerator();
        }

        public bool Remove(string key)
        {
            return _values.Remove(key);
        }

        public bool Remove(KeyValuePair<string, object?> item)
        {
            return ((ICollection<KeyValuePair<string, object?>>) _values).Remove(item);
        }

        public bool TryGetValue(string key, out object? value)
        {
            return _values.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
