namespace Pawnee.Core
{
    using System.Collections.Generic;
    using System.Linq;

    public class PriorityQueue<TKey, TValue>
    {
        private readonly SortedDictionary<TKey, List<TValue>> _heap = new SortedDictionary<TKey, List<TValue>>();

        public PriorityQueue(IComparer<TKey> comparer = null)
        {
            _heap = comparer != null ? new SortedDictionary<TKey, List<TValue>>(comparer)
                                     : new SortedDictionary<TKey, List<TValue>>();
        }

        public void Enqueue(TKey key, TValue value)
        {
            if (!_heap.TryGetValue(key, out var list))
            {
                list = _heap[key] = new List<TValue>();
            }

            list.Add(value);
        }

        public bool Dequeue(out TKey key, out TValue value)
        {
            var min = _heap.FirstOrDefault();
            if (min.Value == null || min.Value.Count == 0)
            {
                key = default(TKey);
                value = default(TValue);
                return false;
            }

            key = min.Key;

            var endOfList = min.Value.Count - 1;
            value = min.Value[endOfList];
            min.Value.RemoveAt(endOfList);

            if (min.Value.Count == 0)
            {
                _heap.Remove(key);
            }

            return true;
        }
    }
}
