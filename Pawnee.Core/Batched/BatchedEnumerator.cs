namespace Pawnee.Core.Batched
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface IBatchedEnumerator<out T> : IDisposable
    {
        IEnumerable<T> CurrentBatch { get; }

        Task<bool> GetNextBatch(int required);
    }

    public interface IBatchedEnumerable<out T>
    {
        IBatchedEnumerator<T> GetEnumerator();
    }
}
