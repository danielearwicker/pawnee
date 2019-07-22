using System;
using System.Threading.Tasks;

namespace Pawnee.Core.Queue
{
    public static class PawneeQueueStorageExtensions
    {
        public static Task InTransaction(this IPawneeQueueStorage storage,
                                         Action<IPawneeQueueState> operation)
        {
            return storage.InTransaction(state =>
            {
                operation(state);
                return true;
            });
        }

        public static async Task<T> InTransaction<T>(this IPawneeQueueStorage storage,
                                                     Func<IPawneeQueueState, T> operation)
            => await Retry.Async(null, 100, TimeSpan.FromSeconds(0.5), nameof(InTransaction), async () =>
            {
                var state = await storage.Read();
                var result = operation(state);
                if (!await state.Commit()) throw new InvalidOperationException("Commit failed");
                return result;
            });
    }
}
