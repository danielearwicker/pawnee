using System.Collections.Generic;

namespace BlobMap
{
    public static class CollectionExtensions
    {
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key)
        {
            dict.TryGetValue(key, out var value);
            return value;
        }

        public static (int Index, bool Found) BinarySearch<T>(this IReadOnlyList<T> list, T value, IComparer<T> comparer = null)
        {
            return BinarySearch(list, 0, list.Count, value, comparer);
        }

        public static (int Index, bool Found) BinarySearch<T>(this IReadOnlyList<T> list, int index, int length,
                T value, IComparer<T> comparer = null)
        {
            if (comparer == null)
                comparer = Comparer<T>.Default;

            int lo = index;
            int hi = index + length - 1;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);
                int order = comparer.Compare(list[i], value);

                if (order == 0) return (i, true);
                if (order < 0)
                    lo = i + 1;
                else
                    hi = i - 1;
            }
            return (lo, false);
        }
    }
}
