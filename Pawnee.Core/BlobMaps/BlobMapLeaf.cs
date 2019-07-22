namespace Pawnee.Core.BlobMaps
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using MessagePack;

    public class BlobMapLeaf<T> : BlobMapNode<T>
    {
        private LinkedListNode<BlobMapLeaf<T>> _loaded;
        private List<string> _sortedKeyCache;

        public IReadOnlyList<string> GetSortedKeys()
        {
            if (!IsLoaded) throw new InvalidOperationException("Must be loaded first");

            if (_sortedKeyCache == null)
            {
                using (Tree.Timings.Track("leaf-sort-keys"))
                {
                    var keys = ValuesByKey.Keys.ToList();
                    keys.Sort(StringComparer.OrdinalIgnoreCase);
                    _sortedKeyCache = keys;
                }
            }

            return _sortedKeyCache;
        }

        public BlobMapLeaf(IBlobMapInternal<T> manager, IBlobMapBranch<T> parent, long id)
            : base(manager, parent, id) { }

        public Dictionary<string, byte[]> ValuesByKey { get; private set; }

        public override bool IsLoaded => ValuesByKey != null;

        public int Locked { get; private set; }

        public override async Task Save()
        {
            if (IsLoaded && IsDirty)
            {
                var bytes = LZ4MessagePackSerializer.Serialize(ValuesByKey);
                await Tree.Store(Id, bytes);
                IsDirty = false;
            }
        }

        public async Task<BlobMapLeaf<T>> Add(string key, byte[] value)
        {
            using (Tree.Timings.Track("branch-add"))
            {
                SetDirty();

                if (ValuesByKey.Count < Tree.MaxNodeKeys)
                {
                    ValuesByKey.Add(key, value);
                    _sortedKeyCache = null;
                    return this;
                }

                var count = ValuesByKey.Count;
                int middle = count / 2;

                var keys = GetSortedKeys();

                var middleKey = keys[middle];

                try
                {
                    Locked++;

                    var newPeer = new BlobMapLeaf<T>(Tree, Parent, Tree.AllocateId());
                    await newPeer.Init();
                    newPeer.IsDirty = true;

                    for (var n = middle; n < count; n++)
                    {
                        var k = keys[n];
                        if (ValuesByKey.TryGetValue(k, out var v))
                        {
                            newPeer.ValuesByKey.Add(k, v);
                            ValuesByKey.Remove(k);
                        }
                    }

                    _sortedKeyCache = null;

                    await Parent.Add(middleKey, newPeer);

                    if (string.Compare(key, middleKey, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        newPeer.ValuesByKey.Add(key, value);
                        return newPeer;
                    }
                    else
                    {
                        ValuesByKey.Add(key, value);
                        return this;
                    }
                }
                finally
                {
                    Locked--;
                }
            }
        }

        public override async Task Load()
        {
            if (!IsLoaded)
            {
                using (Tree.Timings.Track("leaf-load"))
                {
                    // Best to force our parent to stay loaded, AddLoadedLeaf might kill it!
                    ValuesByKey = new Dictionary<string, byte[]>();
                    _sortedKeyCache = null;

                    _loaded = await Tree.AddLoadedLeaf(this);

                    byte[] bytes;
                    using (Tree.Timings.Track("leaf-load-fetch"))
                    { 
                        bytes = await Tree.Fetch(Id);
                    }

                    if (bytes != null)
                    {
                        using (Tree.Timings.Track("leaf-load-deserialize"))
                        {
                            var values = LZ4MessagePackSerializer.Deserialize<Dictionary<string, byte[]>>(bytes);
                            ValuesByKey = values ?? throw new Exception($"Failed to deserialize leaf {Id}");
                        }
                    }
                }
            }
        }

        public async Task UnloadSelf()
        {
            await Save();

            if (_loaded != null)
            {
                Tree.RemoveLoadedLeaf(_loaded);
                _loaded = null;
            }

            _sortedKeyCache = null;
            ValuesByKey = null;

            await Parent.UnloadParentChain();
        }

        public async Task Init()
        {
            if (!IsLoaded)
            {
                ValuesByKey = new Dictionary<string, byte[]>();
                _sortedKeyCache = null;

                _loaded = await Tree.AddLoadedLeaf(this);
            }
        }
        
        public static T Deserialize(byte[] data) => typeof(T) == typeof(byte[])
            ? (T)(object)data
            : MessagePackSerializer.Deserialize<T>(data);

        private class KeyImpl : IBlobMapKey<T>
        {
            private BlobMapLeaf<T> _owner;
            private byte[] _value;

            public bool HasValue { get; private set; }

            public string Key { get; }

            public KeyImpl(BlobMapLeaf<T> owner, string key, bool hasValue, byte[] value)
            {
                _owner = owner;
                _value = value;

                Key = key;
                HasValue = hasValue;                
            }

            public T GetValue() => Deserialize(_value);

            public Task UpdateValue(T value) => UpdateRawValue(
                typeof(T) == typeof(byte[])
                ? (byte[])(object)value
                : MessagePackSerializer.Serialize(value));

            public async Task UpdateRawValue(byte[] value)
            {
                if (!_owner.IsLoaded)
                {
                    throw new InvalidOperationException("Not loaded!");
                }

                if (!_owner.ValuesByKey.ContainsKey(Key))
                {
                    _owner = await _owner.Add(Key, value);
                }
                else
                {
                    _owner.ValuesByKey[Key] = value;
                    _owner.SetDirty();
                }

                _value = value;
                HasValue = true;
            }
        }

        private async Task EnsureLoaded()
        {
            if (!IsLoaded)
            {
                await Load();
            }
            else if (_loaded != null)
            {
                Tree.Touch(_loaded);
            }
        }

        public override async Task<IBlobMapKey<T>> GetKey(string key)
        {
            await EnsureLoaded();

            var hasValue = ValuesByKey.TryGetValue(key, out var value);

            return new KeyImpl(this, key, hasValue, value);
        }

        public override async Task<IReadOnlyBlobMapKey<T>> GetReadOnlyKey(string key)
            => await GetKey(key);

        public override async Task<bool> DeleteKey(string key)
        {
            await EnsureLoaded();

            ValuesByKey.Remove(key);

            return ValuesByKey.Count == 0;
        }

        public override void GatherStats(BlobMapStats stats)
        {
            if (ValuesByKey != null)
            {
                stats.LeafKeysLoaded += ValuesByKey.Count;
                stats.LeafNodesLoaded++;
            }
            else
            {
                stats.LeafNodesUnloaded++;
            }
        }

        public override void PrintTree(TextWriter output, int depth = 0)
        {
            if (ValuesByKey == null)
            {
                output.WriteLine(new string(' ', depth * 2) + $"(unloaded leaf: ID {Id})");
                return;
            }

            foreach (var val in ValuesByKey)
            {
                var key = val.Key == string.Empty ? "(empty)" : val.Key;
                output.WriteLine(new string(' ', depth * 2) + $"{key} = {val.Value}");
            }
        }

        public override Task<bool> UnloadIfPossible()
        {
            return Task.FromResult(!IsLoaded);
        }        
    }
}
