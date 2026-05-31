using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class RecordingLoggerProvider : ILoggerProvider
{
    private readonly List<LogEntry> _entries = [];

    public IReadOnlyList<LogEntry> Entries => _entries;

    public ILogger CreateLogger(string categoryName)
    {
        return new RecordingLogger(categoryName, _entries);
    }

    public void Dispose() { }

    public sealed record LogEntry(
        string CategoryName,
        LogLevel LogLevel,
        string Message,
        Exception? Exception,
        IReadOnlyDictionary<string, object?> Fields
    );

    private sealed class RecordingLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly List<LogEntry> _entries;

        public RecordingLogger(string categoryName, List<LogEntry> entries)
        {
            _categoryName = categoryName;
            _entries = entries;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            return NullScope.Instance;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            var fields = state is IEnumerable<KeyValuePair<string, object?>> values ?
                values
                   .Where(static value => value.Key != "{OriginalFormat}")
                   .ToDictionary(static value => value.Key, static value => value.Value, StringComparer.Ordinal) :
                new Dictionary<string, object?>(StringComparer.Ordinal);

            _entries.Add(new LogEntry(_categoryName, logLevel, formatter(state, exception), exception, fields));
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new ();

        public void Dispose() { }
    }
}
