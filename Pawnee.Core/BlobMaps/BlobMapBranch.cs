namespace Pawnee.Core.BlobMaps
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using MessagePack;
    using MessagePack.Formatters;
    using MessagePack.Resolvers;

    public interface IBlobMapBranch<T> : IBlobMapNode<T>
    {
        Task Add(string startKey, IBlobMapNode<T> child);
        Task UnloadParentChain();        
    }

    [MessagePackObject]
    public struct LevelReader
    {
        [Key(0)]
        public int Level;
    }

    [MessagePackObject]
    public class BlobMapBranch<T> : BlobMapNode<T>, IBlobMapBranch<T>
    {
        [IgnoreMember]
        private List<string> _sortedKeyCache;

        public IReadOnlyList<string> GetSortedKeys()
        {
            if (!IsLoaded) throw new InvalidOperationException("Must be loaded first");

            if (_sortedKeyCache == null)
            {
                using (Tree.Timings.Track("branch-sort-keys"))
                {
                    var keys = ChildNodesByKey.Keys.ToList();
                    keys.Sort(StringComparer.OrdinalIgnoreCase);
                    _sortedKeyCache = keys;
                }
            }

            return _sortedKeyCache;
        }

        public BlobMapBranch(IBlobMapInternal<T> manager, IBlobMapBranch<T> parent, long id)
            : base(manager, parent, id) { }

        [SerializationConstructor]
        public BlobMapBranch(int level, long highestId, Dictionary<string, IBlobMapNode<T>> childNodes)
            : base(null, null, 0)
        {
            Level = level;
            HighestId = highestId;
            ChildNodesByKey = childNodes;
        }

        [Key(0)]
        public int Level { get; private set; } = -1;

        [Key(1)]
        public long HighestId { get; private set; }

        [Key(2)]
        public Dictionary<string, IBlobMapNode<T>> ChildNodesByKey { get; private set; }

        public long AllocateId()
        {
            SetDirty();
            return ++HighestId;
        }

        public void Clear()
        {
            SetDirty();
            ChildNodesByKey = null;
            _sortedKeyCache = null;
        }

        [IgnoreMember]
        public override bool IsLoaded => ChildNodesByKey != null;

        private class CustomNodeResolver : IFormatterResolver
        {
            private readonly Func<long, IBlobMapNode<T>> _createNode;

            public CustomNodeResolver(Func<long, IBlobMapNode<T>> createNode) => _createNode = createNode;

            private class Formatter : IMessagePackFormatter<IBlobMapNode<T>>
            {
                private readonly Func<long, IBlobMapNode<T>> _createNode;

                public Formatter(Func<long, IBlobMapNode<T>> createNode) => _createNode = createNode;
                
                public IBlobMapNode<T> Deserialize(byte[] bytes, int offset, IFormatterResolver formatterResolver, out int readSize)
                    => _createNode(MessagePackBinary.ReadInt64(bytes, offset, out readSize));
                
                public int Serialize(ref byte[] bytes, int offset, IBlobMapNode<T> value, IFormatterResolver formatterResolver)
                    => MessagePackBinary.WriteInt64(ref bytes, offset, value.Id);
            }

            public IMessagePackFormatter<TFor> GetFormatter<TFor>()
            {
                if (typeof(TFor) == typeof(IBlobMapNode<T>))
                {
                    return (IMessagePackFormatter<TFor>)new Formatter(_createNode);
                }

                return StandardResolver.Instance.GetFormatter<TFor>();
            }
        }

        public override async Task Save()
        {
            using (Tree.Timings.Track("branch-save"))
            {
                if (!IsLoaded) return;

                await Task.WhenAll(ChildNodesByKey.Values.Select(c => c.Save()));

                if (!IsDirty) return;

                var bytes = LZ4MessagePackSerializer.Serialize(this, new CustomNodeResolver(null));

                if (Level == 0)
                {
                    await Tree.FlushPendingSaves();
                }

                await Tree.Store(Id, bytes);
                IsDirty = false;
            }
        }

        public void Init()
        {
            ChildNodesByKey = new Dictionary<string, IBlobMapNode<T>>();
        }

        public override async Task Load()
        {
            if (IsLoaded) return;

            using (Tree.Timings.Track("branch-load"))
            {
                var blob = await Tree.Fetch(Id);
                if (blob == null)
                {
                    if (Parent == null)
                    {
                        // Root being used for first time: create a single empty leaf
                        Level = 0;

                        Init();

                        var child = new BlobMapLeaf<T>(Tree, this, Tree.AllocateId());
                        ChildNodesByKey.Add(string.Empty, child);
                        child.OnCreate();
                    }
                    return;
                }

                Level = LZ4MessagePackSerializer.Deserialize<LevelReader>(blob).Level;

                var resolver = new CustomNodeResolver(id => Level == 0 ? (IBlobMapNode<T>)new BlobMapLeaf<T>(Tree, this, id)
                                                                       : new BlobMapBranch<T>(Tree, this, id));

                var loaded = LZ4MessagePackSerializer.Deserialize<BlobMapBranch<T>>(blob, resolver);

                HighestId = loaded.HighestId;
                ChildNodesByKey = loaded.ChildNodesByKey;
            }
        }

        public async Task Add(string startKey, IBlobMapNode<T> child)
        {
            using (Tree.Timings.Track("branch-add"))
            {
                SetDirty();

                void Attach(BlobMapBranch<T> target, string key, IBlobMapNode<T> node)
                {
                    node.ReplaceParent(target);
                    target.ChildNodesByKey.Add(key, node);
                    _sortedKeyCache = null;
                }

                if (!IsLoaded || ChildNodesByKey.Count < Tree.MaxNodeKeys)
                {
                    Attach(this, startKey, child);
                    return;
                }

                var count = ChildNodesByKey.Count;
                int middle = count / 2;

                var keys = GetSortedKeys();

                var middleKey = keys[middle];

                if (Parent != null)
                {
                    var newPeer = new BlobMapBranch<T>(Tree, Parent, Tree.AllocateId());
                    newPeer.Init();
                    newPeer.Level = Level;
                    newPeer.IsDirty = true;

                    for (var n = middle; n < count; n++)
                    {
                        var k = keys[n];
                        if (ChildNodesByKey.TryGetValue(k, out var v))
                        {
                            Attach(newPeer, k, v);
                            ChildNodesByKey.Remove(k);
                        }
                    }

                    if (string.Compare(startKey, middleKey, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        Attach(newPeer, startKey, child);
                    }
                    else
                    {
                        Attach(this, startKey, child);
                    }

                    await Parent.Add(middleKey, newPeer);
                    await Parent.UnloadIfPossible();
                    return;
                }

                BlobMapBranch<T> Create()
                {
                    var branch = new BlobMapBranch<T>(Tree, this, Tree.AllocateId());
                    branch.Init();
                    branch.Level = Level;
                    branch.IsDirty = true;
                    return branch;
                }

                void Copy(int start, int end, BlobMapBranch<T> target)
                {
                    for (var n = start; n < end; n++)
                    {
                        var k = keys[n];
                        if (ChildNodesByKey.TryGetValue(k, out var v))
                        {
                            Attach(target, k, v);
                        }
                    }
                }

                var lower = Create();
                Copy(0, middle, lower);

                var higher = Create();
                Copy(middle, count, higher);

                ChildNodesByKey.Clear();
                Attach(this, string.Empty, lower);
                Attach(this, middleKey, higher);
                Level++;

                if (string.Compare(startKey, middleKey, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    Attach(higher, startKey, child);
                }
                else
                {
                    Attach(lower, startKey, child);
                }

                await UnloadIfPossible();
            }
        }

        public int GetLowerBoundIndex(string key)
        {
            using (Tree.Timings.Track("branch-getlowerboundindex"))
            {
                var keys = GetSortedKeys();

                var (Index, Found) = keys.BinarySearch(key, StringComparer.OrdinalIgnoreCase);
                return Found ? Index : Index - 1;
            }
        }

        public string GetLowerBound(string key)
        {
            var keys = GetSortedKeys();
            var index = GetLowerBoundIndex(key);            
            return keys[index];
        }

        public override async Task<IBlobMapKey<T>> GetKey(string key)
        {
            if (!IsLoaded) await Load();
            var lowerBound = GetLowerBound(key);
            return await ChildNodesByKey[lowerBound].GetKey(key);
        }

        public override async Task<IReadOnlyBlobMapKey<T>> GetReadOnlyKey(string key) => await GetKey(key);
       
        public override async Task<bool> DeleteKey(string key)
        {
            if (!IsLoaded) await Load();
            var lowerBound = GetLowerBound(key);
            var child = ChildNodesByKey[lowerBound];
            if (await child.DeleteKey(key))
            {
                Tree.AddGarbage(child.Id);
                ChildNodesByKey.Remove(key);
            }

            return ChildNodesByKey.Count == 0;
        }

        public override async Task<bool> UnloadIfPossible()
        {
            if (ChildNodesByKey == null) return true;
            
            var childResults = await Task.WhenAll(ChildNodesByKey.Values.Select(c => c.UnloadIfPossible()));
            if (!childResults.All(c => c)) return false;

            if (Level == 0) return false;

            await Save();

            ChildNodesByKey = null;
            return true;
        }

        public async Task UnloadParentChain()
        {
            await UnloadIfPossible();

            if (Parent != null)
            {
                await Parent.UnloadParentChain();
            }
        }

        public override void GatherStats(BlobMapStats stats)
        {
            if (ChildNodesByKey != null)
            {
                stats.BranchNodesLoaded++;

                foreach (var child in ChildNodesByKey)
                {
                    child.Value.GatherStats(stats);
                }
            }
            else
            {
                stats.BranchNodesUnloaded++;
            }
        }

        public override void PrintTree(TextWriter output, int depth = 0)
        {
            if (ChildNodesByKey == null)
            {
                output.WriteLine(new string(' ', depth * 2) + $"(unloaded branch: ID {Id})");
                return;
            }

            foreach (var child in ChildNodesByKey)
            {
                var key = child.Key == string.Empty ? "(empty)" : child.Key;
                output.WriteLine(new string(' ', depth * 2) + $"-> {key}");
                child.Value.PrintTree(output, depth + 1);
            }
        }        
    }
}
