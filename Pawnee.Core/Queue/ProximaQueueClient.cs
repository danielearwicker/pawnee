using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Pawnee.Core.Queue
{
    public interface IPawneeQueueClient
    {
        IPawneeQueueItem Enqueue<T>(IPawneeQueueState state,
                                     string stage,
                                     Expression<Func<T, Task>> expression,
                                     Guid? batch = null,
                                     Guid? afterBatch = null,
                                     string fromStage = null);

        Task Invoke(IPawneeQueueItem item, params object[] extraServices);
    }

    public class PawneeQueueClient : IPawneeQueueClient
    {
        private readonly IMethodCallSerializer _methodCalls;

        public PawneeQueueClient(IMethodCallSerializer methodCalls)
            => _methodCalls = methodCalls;

        public IPawneeQueueItem Enqueue<T>(
            IPawneeQueueState state,
            string stage,
            Expression<Func<T, Task>> expression,
            Guid? batch = null,
            Guid? afterBatch = null,
            string fromStage = null)
                => state.Enqueue(stage, _methodCalls.Capture(expression), batch, afterBatch, fromStage);

        public Task Invoke(IPawneeQueueItem item, params object[] extraServices)
            => _methodCalls.Invoke(item.Content, extraServices);
    }

}
