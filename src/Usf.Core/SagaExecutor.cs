using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.DatabaseAccess;

namespace Usf.Core;

public class Ticket<TTicketId, TTicketData>
    where TTicketId : IEquatable<TTicketId>, IComparable<TTicketId>
{
    public TTicketId Id { get; set; }
    public TTicketData Data { get; set; }
    public SagaDefinition SagaDefinition { get; set; }
}

public class SagaDefinition
{
    public List<SagaStep> Steps { get; set; }
}

public class SagaStep
{
    public required string Type { get; init; }
}

public class SagaExecutor<TTicket, TTicketId, TTicketData>
    where TTicket : ITicket<TTicketId, TTicketData>
{
    private readonly ISagaTicketSession<TTicket, TTicketId, TTicketData> _session;
    private readonly IStepScopeFactory _stepScopeFactory;

    public SagaExecutor(
        ISagaTicketSession<TTicket, TTicketId, TTicketData> session,
        INextStepFinder nextStepFinder,
        IStepScopeFactory stepScopeFactory
    )
    {
        _session = session;
        _stepScopeFactory = stepScopeFactory;
    }

    public async Task ExecuteStepAsync(TTicketId ticketId, CancellationToken cancellationToken = default)
    {
        // 1. Load Saga Ticket
        var sagaTicket = await _session.LoadSagaTicketAsync(ticketId, cancellationToken);
        // 2. Instantiate Saga
        var type = sagaTicket.GetNextStepType();

        // 3. Identify and instantiate Saga Step
        await using var scope = _stepScopeFactory.CreateScope();
        var step = scope.CreateStep(type, sagaTicket);

        // 4. Execute Saga Step
        sagaTicket = await step.ExecuteAsync(sagaTicket, cancellationToken);

        // 5. Update Saga Ticket
        await _session.UpdateSagaTicketAsync(sagaTicket, cancellationToken);
        await _session.SaveChangesAsync(cancellationToken);
    }
}

public interface ITicket<TTicketId, TTicketData> { }

public sealed class MongoDbTicket<TTicketData> : ITicket<Guid, TTicketData> { }

public interface IStepScopeFactory
{
    IStepScope CreateScope();
}

public interface IStepScope : IAsyncDisposable
{
    ISagaStep<TTicketData> CreateStep<TTicketData>(Type type, ITicket<TTicketData> sagaTicket);
}

public sealed class DiStepScopeFactory : IStepScopeFactory
{
    private readonly IServiceProvider _serviceProvider;

    public DiStepScopeFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public IStepScope CreateScope() => throw new NotImplementedException();
}

public interface ISagaStep<TTicketData>
{
    Task<Ticket<TTicketId, TTicketData>> ExecuteAsync(
        Ticket<TTicketId, TTicketData> sagaTicket,
        CancellationToken cancellationToken
    );
}

public interface ISagaTicketSession<TTicket, in TTicketId, TTicketData> : ISession
    where TTicket : ITicket<TTicketId, TTicketData>
{
    Task<TTicket> LoadSagaTicketAsync(TTicketId sagaId, CancellationToken cancellationToken);
    Task UpdateSagaTicketAsync(TTicket sagaTicket, CancellationToken cancellationToken);
}
