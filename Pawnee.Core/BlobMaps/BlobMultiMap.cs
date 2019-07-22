namespace Pawnee.Core.BlobMaps
{
    using System;
    using System.Threading.Tasks;
    using Batched;
    using MessagePack;

    [MessagePackObject]
    public class MultiMapKey
    {
        [Key(0)]
        public string Value;
        [Key(1)]
        public string Id;

        public static string GenerateId() => Guid.NewGuid().ToString("N");

        [SerializationConstructor]
        public MultiMapKey(string value, string id)
        {
            Value = value;
            Id = id;
        }

        public MultiMapKey(string str)
        {
            int dash = str.LastIndexOf('-');
            if (dash == -1)
                throw new ArgumentOutOfRangeException(nameof(str), str);

            Value = str.Substring(0, dash);
            Id = str.Substring(dash + 1);
        }

        public override string ToString() => $"{Value}-{Id}";
    }

    public interface IBlobMultiMap<T>
    {
        Task<MultiMapKey> Add(string key, T value);

        Task Delete(MultiMapKey key);

        Task Upsert(MultiMapKey key, T value);

        Task UpsertRaw(MultiMapKey key, byte[] bytes);

        IBatchedEnumerator<(MultiMapKey Key, T Value)> IterateKey(string key);

        IBatchedEnumerator<(MultiMapKey Key, T Value)> IterateKeyRange(string from, string until);

        IBatchedEnumerator<(MultiMapKey Key, T Value)> IterateAll();

        Task SaveOverwrite();

        Task Clear();
    }

    public class BlobMultiMap<T> : IBlobMultiMap<T>
    {
        private readonly IBlobMap<T> _map;

        public BlobMultiMap(IBlobMap<T> map) => _map = map;

        public async Task<MultiMapKey> Add(string key, T value)
        {
            var fullKey = new MultiMapKey(key, MultiMapKey.GenerateId());
            await (await _map.GetKey(fullKey.ToString())).UpdateValue(value);
            return fullKey;
        }

        public Task Clear() => _map.Clear();

        public Task SaveOverwrite() => _map.SaveOverwrite();

        public Task Delete(MultiMapKey key) => _map.DeleteKey(key.ToString());

        public IBatchedEnumerator<(MultiMapKey Key, T Value)> IterateAll()
            => _map.IterateKeyValues(string.Empty)
                   .Select(k => (new MultiMapKey(k.Key), k.Value));

        public IBatchedEnumerator<(MultiMapKey Key, T Value)> IterateKey(string key)
            => _map.IterateKeyValues(new MultiMapKey(key, string.Empty).ToString(),
                                     k => new MultiMapKey(k).Value == key)
                   .Select(k => (Key: new MultiMapKey(k.Key), k.Value));

        public IBatchedEnumerator<(MultiMapKey Key, T Value)> IterateKeyRange(string from, string until)
            => _map.IterateKeyValues(new MultiMapKey(from, string.Empty).ToString(),
                                     until == null ? null : new Func<string, bool>(
                                         k => StringComparer.OrdinalIgnoreCase.Compare(new MultiMapKey(k).Value, until) < 0))
                   .Select(k => (Key: new MultiMapKey(k.Key), k.Value));

        public async Task Upsert(MultiMapKey key, T value)
            => await (await _map.GetKey(key.ToString())).UpdateValue(value);

        public async Task UpsertRaw(MultiMapKey key, byte[] bytes)
            => await (await _map.GetKey(key.ToString())).UpdateRawValue(bytes);
    }
}
