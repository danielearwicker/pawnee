using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Platform
{
    public class BatchedTransform<TIn, TOut> : IBatchedEnumerator<TOut>
    {
        private readonly IBatchedEnumerator<TIn> _source;
        private readonly Func<TIn, IEnumerable<TOut>> _transformer;
        private readonly Func<int, int> _take;

        public IEnumerable<TOut> CurrentBatch { get; private set; }

        public BatchedTransform(IBatchedEnumerator<TIn> source, 
                                Func<TIn, IEnumerable<TOut>> transformer,
                                Func<int, int> take = null)
        {
            _source = source;
            _transformer = transformer;
            _take = take;
        }

        public void Dispose()
        {
            _source.Dispose();
        }

        public async Task<bool> GetNextBatch(int required)
        {
            var take = _take == null ? required : _take(required);
            if (take == 0 || !await _source.GetNextBatch(take)) return false;

            CurrentBatch = _source.CurrentBatch.SelectMany(_transformer);
            return true;
        }
    }

    public class BatchedConcat<T> : IBatchedEnumerator<T>
    {
        private readonly IReadOnlyList<IBatchedEnumerator<T>> _sources;

        private int _currentSourceIndex;

        private IBatchedEnumerator<T> CurrentSource => _currentSourceIndex >= _sources.Count
                                                       ? null : _sources[_currentSourceIndex];

        public BatchedConcat(IReadOnlyList<IBatchedEnumerator<T>> sources)
        {
            _sources = sources;
        }

        public IEnumerable<T> CurrentBatch => CurrentSource?.CurrentBatch ?? Enumerable.Empty<T>();

        public void Dispose()
        {
            foreach (var source in _sources) source.Dispose();
        }

        public async Task<bool> GetNextBatch(int required)
        {
            while (CurrentSource != null && !await CurrentSource.GetNextBatch(required))
            {
                _currentSourceIndex++;
            }

            return CurrentSource != null;
        }
    }

    public class BatchedLookAhead<T> : IBatchedEnumerator<T>
    {
        private readonly IBatchedEnumerator<T> _source;
        private Task<IEnumerable<T>> _lookAhead;

        public IEnumerable<T> CurrentBatch { get; private set; } = Enumerable.Empty<T>();

        public BatchedLookAhead(IBatchedEnumerator<T> source) => _source = source;
        
        public void Dispose() { }

        public async Task<bool> GetNextBatch(int required)
        {
            if (_lookAhead == null)
            {
                _lookAhead = GetNextBatchImpl(required);
            }

            var result = _lookAhead;

            var next = new TaskCompletionSource<IEnumerable<T>>();

            _ = result.ContinueWith(_ =>
            {
                Task.Run(() =>
                {
                    try
                    {
                        var batch = GetNextBatchImpl(required).Result;
                        next.SetResult(batch);
                    }
                    catch (Exception x)
                    {
                        next.SetException(x);
                    }
                });
            });

            _lookAhead = next.Task;

            CurrentBatch = await result;
            return CurrentBatch != null;
        }

        private async Task<IEnumerable<T>> GetNextBatchImpl(int required)
            => (!await _source.GetNextBatch(required)) ? null : _source.CurrentBatch;        
    }

    public static class BatchedLinq
    {
        public static IBatchedEnumerator<TOut> Select<TIn, TOut>(this IBatchedEnumerator<TIn> source, Func<TIn, TOut> selector)
            => new BatchedTransform<TIn, TOut>(source, i => new[] { selector(i) });

        public static IBatchedEnumerator<TOut> SelectMany<TIn, TOut>(this IBatchedEnumerator<TIn> source, Func<TIn, IEnumerable<TOut>> selector)
            => new BatchedTransform<TIn, TOut>(source, selector);

        public static IBatchedEnumerator<T> Where<T>(this IBatchedEnumerator<T> source, Func<T, bool> predicate)
            => new BatchedTransform<T, T>(source, i => predicate(i) ? new[] { i } : Enumerable.Empty<T>());

        public static IBatchedEnumerator<T> TakeWhile<T>(this IBatchedEnumerator<T> source, Func<T, bool> predicate)
        {
            var more = true;
            return new BatchedTransform<T, T>(source, i => (more = more && predicate(i)) 
                                                            ? new[] { i } : Enumerable.Empty<T>(),
                                                            t => more ? t : 0);
        }

        public static async Task<T> First<T>(this IBatchedEnumerator<T> source)
        {
            while (await source.GetNextBatch(1))
            {
                foreach (var item in source.CurrentBatch)
                {
                    return item;
                }
            }

            throw new InvalidOperationException("Unexpected empty collection");
        }

        public static IBatchedEnumerator<T> Take<T>(this IBatchedEnumerator<T> source, int take)
        {
            return new BatchedTransform<T, T>(source,
                i =>
                {
                    if (take <= 0) return Enumerable.Empty<T>();
                    take--;
                    return new[] { i };
                }, 
                t => Math.Min(t, take));
        }

        public static IBatchedEnumerator<T> Concat<T>(this IEnumerable<IBatchedEnumerator<T>> sources)
            => new BatchedConcat<T>(sources.ToList());

        public static IBatchedEnumerator<T> Merge<T>(this IEnumerable<IBatchedEnumerator<T>> sources,
                                                     Func<T, T, int> compare = null)
            => new BatchedMerger<T>(sources.ToList(), compare);

        public static IBatchedEnumerator<T> LookAhead<T>(this IBatchedEnumerator<T> source)
            => new BatchedLookAhead<T>(source);

        public static async Task<T> Aggregate<T>(this IBatchedEnumerator<T> source,
                                                 Func<T, T, T> each)
        {
            T accumulator = default;
            var started = false;

            while (await source.GetNextBatch(DefaultBatchSize))
            {
                foreach (var item in source.CurrentBatch)
                {
                    if (!started)
                    {
                        accumulator = item;
                        started = true;
                    }
                    else
                    {
                        accumulator = each(accumulator, item);
                    }
                }
            }

            if (!started) throw new InvalidOperationException("Unexpected empty collection");
            return accumulator;
        }

        public static async Task<T> Aggregate<T>(this IBatchedEnumerator<T> source,
                                                 T initializer,
                                                 Func<T, T, T> each)
        {
            T accumulator = initializer;

            while (await source.GetNextBatch(DefaultBatchSize))
            {
                foreach (var item in source.CurrentBatch)
                {
                    accumulator = each(accumulator, item);
                }                    
            }

            return accumulator;
        }

        public static async Task<List<T>> ToList<T>(this IBatchedEnumerator<T> source)
        {
            var result = new List<T>();

            while (await source.GetNextBatch(DefaultBatchSize))
            {
                foreach (var item in source.CurrentBatch)
                {
                    result.Add(item);
                }
            }

            return result;
        }

        private const int DefaultBatchSize = 10000;
    }
}
