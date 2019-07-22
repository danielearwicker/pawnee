namespace Pawnee.Core.BlobMaps
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public static class BlobMapExtensions
    {
        public static async Task Set<TValue>(this IBlobMap<TValue> tree, string key, TValue value)
        {
            var entry = await tree.GetKey(key);
            await entry.UpdateValue(value);
        }

        public static Task MutateDictionary<TKey, TValue>(this IBlobMap<Dictionary<TKey, TValue>> tree,
                                                          string key,
                                                          Func<Dictionary<TKey, TValue>, bool> updater)            
            => tree.MutateCollection<KeyValuePair<TKey, TValue>, 
                                     Dictionary<TKey, TValue>>(key, updater);

        public static Task MutateList<TValue>(this IBlobMap<List<TValue>> tree, string key, Func<List<TValue>, bool> updater)
            => tree.MutateCollection<TValue, List<TValue>>(key, updater);

        public static Task AddToList<TValue>(this IBlobMap<List<TValue>> tree, string key, TValue value)
            => tree.MutateList<TValue>(key, list => { list.Add(value); return true; });

        public static Task MutateCollection<TValue, TCollection>(this IBlobMap<TCollection> tree,
                                          string key,
                                          Func<TCollection, bool> updater)
            where TCollection : ICollection<TValue>, new()
            => tree.Mutate(key, updater, s => s.Count == 0);
        
        public static async Task Mutate<TValue>(this IBlobMap<TValue> tree,
                                                string key,
                                                Func<TValue, bool> updater,
                                                Func<TValue, bool> canRemove)
            where TValue : new()
        {
            var entry = await tree.GetKey(key);
            if (entry.HasValue)
            {
                var value = entry.GetValue();

                if (!updater(value)) return;

                if (canRemove?.Invoke(value) ?? false)
                {
                    await tree.DeleteKey(key);
                }
                else
                {                    
                    await entry.UpdateValue(value);
                }
            }
            else
            {
                var value = new TValue();
                if (updater(value) && !(canRemove?.Invoke(value) ?? false))
                {
                    await entry.UpdateValue(value);
                }
            }
        }
    }
}
