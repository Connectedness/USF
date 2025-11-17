using System;
using System.Threading.Tasks;
using Chaos.Mongo;

namespace Usf.Persistence.MongoDb;

public abstract class MongoDbClient : IAsyncDisposable, IDisposable
{
    protected MongoDbClient(IMongoHelper mongoHelper)
    {
        MongoHelper = mongoHelper;
    }

    public IMongoHelper MongoHelper { get; }

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public virtual void Dispose() { }
}
