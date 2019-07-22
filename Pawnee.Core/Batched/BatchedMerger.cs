using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Platform
{
    public class BatchedMerger<T> : IBatchedEnumerator<T>
    {
        private class Source
        {
            public IBatchedEnumerator<T> Batches;
            public IEnumerator<T> CurrentBatchEnumerator;
        }

        private readonly PriorityQueue<T, Source> _heap;
        private readonly IEnumerable<IBatchedEnumerator<T>> _sources;
        
        private bool _started;

        private class Comparer : IComparer<T>
        {
            private readonly Func<T, T, int> _compare;

            public Comparer(Func<T, T, int> compare) => _compare = compare;

            public int Compare(T x, T y) => _compare(x, y);
        }

        public BatchedMerger(IEnumerable<IBatchedEnumerator<T>> sources,
                             Func<T, T, int> compare = null)
        {
            _sources = sources;
            _heap = new PriorityQueue<T, Source>(new Comparer(compare ?? Comparer<T>.Default.Compare));
        }

        public IEnumerable<T> CurrentBatch { get; private set; }

        public void Dispose()
        {
            foreach (var source in _sources) source.Dispose();
        }

        public async Task<bool> GetNextBatch(int required)
        {
            if (!_started)
            {
                foreach (var source in _sources)
                {
                    if (await source.GetNextBatch(required))
                    {
                        var firstBatch = source.CurrentBatch.GetEnumerator();
                        if (firstBatch.MoveNext())
                        {
                            _heap.Enqueue(firstBatch.Current, new Source
                            {
                                Batches = source,
                                CurrentBatchEnumerator = firstBatch
                            });
                        }
                    }
                }

                _started = true;
            }

            var batchItems = new List<T>();

            for (var n = 0; n < required; n++)
            {                
                if (!_heap.Dequeue(out var item, out var source))
                {
                    break;
                }

                batchItems.Add(item);

                if (source.CurrentBatchEnumerator.MoveNext())
                {
                    _heap.Enqueue(source.CurrentBatchEnumerator.Current, source);
                }
                else
                {
                    while (await source.Batches.GetNextBatch(required))
                    {
                        var nextBatch = source.Batches.CurrentBatch.GetEnumerator();
                        if (nextBatch.MoveNext())
                        {
                            source.CurrentBatchEnumerator = nextBatch;
                            _heap.Enqueue(nextBatch.Current, source);
                            break;
                        }
                    }
                }
            }

            CurrentBatch = batchItems;

            return batchItems.Count != 0;
        }
    }
}
