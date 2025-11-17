using System;
using System.Threading;
using System.Threading.Tasks;
using Chaos.Mongo;
using MongoDB.Driver;
using Usf.Core.DatabaseAccess;

namespace Usf.Persistence.MongoDb;

public abstract class MongoDbSession : MongoDbClient, ISession
{
    protected MongoDbSession(IMongoHelper mongoHelper, IClientSessionHandle session) : base(mongoHelper)
    {
        Session = session;
    }

    public IClientSessionHandle Session { get; }

    public Task SaveChangesAsync(CancellationToken cancellationToken = default) =>
        Session.CommitTransactionAsync(cancellationToken);

    public override ValueTask DisposeAsync()
    {
        Session.Dispose();
        return ValueTask.CompletedTask;
    }

    public override void Dispose()
    {
        Session.Dispose();
    }
}

public sealed class MongoSagaTicketSession<TTicketId, TTicketData> : MongoDbClient,
                                                                     ISagaTicketSession<TTicketData, TTicketData>
{
    public MongoSagaTicketSession(IMongoHelper mongoHelper) : base(mongoHelper) { }
    public Task SaveChangesAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task<ITicket<TTicketData>> LoadSagaTicketAsync(TTicketData sagaId, CancellationToken cancellationToken)
    {
        MongoHelper.GetCollection<Ticket<TTicketData>>()
    }

    public Task UpdateSagaTicketAsync(ITicket<TTicketData> sagaTicket, CancellationToken cancellationToken) =>
        throw new NotImplementedException();
}
