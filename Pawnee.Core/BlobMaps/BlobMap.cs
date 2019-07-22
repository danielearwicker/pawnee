using Platform;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BlobMap
{
    public interface IBlobMapInternal<T>
    {
        long AllocateId();
        void AddGarbage(long id);

        Task<byte[]> Fetch(long id);
        Task Store(long id, byte[] data);        

        int MaxNodeKeys { get; }

        Task LimitMemoryConsumption();
        Task FlushPendingSaves(int maxCount = 0);

        Task<LinkedListNode<BlobMapLeaf<T>>> AddLoadedLeaf(BlobMapLeaf<T> loaded);
        void RemoveLoadedLeaf(LinkedListNode<BlobMapLeaf<T>> unloaded);
        void Touch(LinkedListNode<BlobMapLeaf<T>> loaded);

        string Name { get; }

        ITimings Timings { get; }
    }

    public interface ISaved
    {
        long RootId { get; }

        Task DeleteGarbage();
    }

    public interface IBlobMap<T>
    {
        Task<IBlobMapKey<T>> GetKey(string key);

        Task DeleteKey(string key);

        Task SaveOverwrite();

        Task<ISaved> SaveNew();

        Task Clear();

        IBatchedEnumerator<(string Key, T Value)> IterateKeyValues(string minKey, Func<string, bool> takeWhile = null);

        long AllocateId();
    }

    public class BlobMapOptions
    {
        public int MaxNodeKeys { get; set; } = 1000;
        public int MaxLeavesLoaded { get; set; } = 1000;
        public string IdPrefix { get; set; }
        public string Name { get; set; }
        public long RootNodeId { get; set; }
    }

    public class BlobMapTree<T> : IBlobMapInternal<T>, IBlobMap<T>
    {
        private readonly LinkedList<BlobMapLeaf<T>> _unloadQueue = new LinkedList<BlobMapLeaf<T>>();
        private readonly string _idPrefix;
        private readonly BlobMapBranch<T> _root;

        private readonly Process _process = Process.GetCurrentProcess();
        private readonly Stopwatch _processRefreshTImer = new Stopwatch();

        private readonly IBlobStorage _storage;
        private ImmutableList<Task> _savingTasks = ImmutableList.Create<Task>();

        private HashSet<long> _garbage = new HashSet<long>();

        public int MaxNodeKeys { get; }
        public int MaxLeavesLoaded { get; }

        public string Name { get; }

        public ITimings Timings { get; }

        public BlobMapStats GatherStats()
        {
            var stats = new BlobMapStats();
            _root.GatherStats(stats);
            return stats;
        }

        public string GenerateTree()
        {
            var stringWriter = new StringWriter();
            _root.PrintTree(stringWriter);
            return stringWriter.ToString();
        }

        public BlobMapTree(
            IBlobStorage storage,
            ITimings timings,
            BlobMapOptions options)
        {
            _idPrefix = options.IdPrefix;
            _processRefreshTImer.Start();

            _storage = storage;
            MaxNodeKeys = options.MaxNodeKeys;
            MaxLeavesLoaded = options.MaxLeavesLoaded;
            Name = options.Name ?? "(unnamed)";

            Timings = timings;

            _root = new BlobMapBranch<T>(this, null, options.RootNodeId);
        }

        public async Task Clear()
        {
            await _storage.Clear();
            _root.Clear();
        }

        void IBlobMapInternal<T>.AddGarbage(long id)
        {
            if (!_garbage.Add(id))
            {
                throw new InvalidOperationException($"Id {id} registered as garbage a second time");
            }
        }

        public Task<IBlobMapKey<T>> GetKey(string key) =>_root.GetKey(key);
        
        public Task DeleteKey(string key) => _root.DeleteKey(key);
        
        public IBatchedEnumerator<(string Key, T Value)> IterateKeyValues(string minKey, Func<string, bool> takeWhile = null)
            => new Cursor<T>(_root, minKey, takeWhile);

        private class SaveResult : ISaved
        {
            private readonly IEnumerable<long> _garbage;
            private readonly BlobMapTree<T> _map;

            public SaveResult(BlobMapTree<T> map)
            {
                _map = map;
                RootId = _map._root.Id;
                _garbage = _map._garbage;
                _map._garbage = new HashSet<long>();
            }

            public long RootId { get; }

            public async Task DeleteGarbage()
            {
                foreach (var id in _garbage)
                {
                    await _map._storage.Delete(_map.FormatId(id));
                }
            }
        }

        public async Task SaveOverwrite()
        {            
            await _root.Save();

            IBlobMapInternal<T> internalThis = this;
            await internalThis.FlushPendingSaves();

            await new SaveResult(this).DeleteGarbage();
        }

        public async Task<ISaved> SaveNew()
        {
            if (_root.IsDirty)
            {
                _root.ChangeId();

                await _root.Save();

                IBlobMapInternal<T> internalThis = this;
                await internalThis.FlushPendingSaves();
            }

            return new SaveResult(this);
        }

        async Task IBlobMapInternal<T>.LimitMemoryConsumption()
        {
            if (_processRefreshTImer.Elapsed.TotalSeconds > 5)
            {
                _processRefreshTImer.Restart();
                _process.Refresh();
                await LimitMemoryConsumptionNow();
            }
        }

        private async Task LimitMemoryConsumptionNow()
        {
            while (_unloadQueue.Count > MaxLeavesLoaded)
            {
                var unloadable = _unloadQueue.FirstOrDefault(l => l.Locked == 0);
                if (unloadable == null)
                {
                    return;
                }

                await unloadable.UnloadSelf();
            }
        }

        async Task<LinkedListNode<BlobMapLeaf<T>>> IBlobMapInternal<T>.AddLoadedLeaf(BlobMapLeaf<T> loaded)
        {
            _process.Refresh();
            await LimitMemoryConsumptionNow();

            return _unloadQueue.AddLast(loaded);
        }

        void IBlobMapInternal<T>.RemoveLoadedLeaf(LinkedListNode<BlobMapLeaf<T>> unloaded)
            => _unloadQueue.Remove(unloaded);

        public long AllocateId() => _root.AllocateId();

        private string FormatId(long id)
            => _idPrefix == null ? id.ToString() : $"{_idPrefix}{id}";
        
        void IBlobMapInternal<T>.Touch(LinkedListNode<BlobMapLeaf<T>> loaded)
        {
            if (loaded.Next != null)
            {
                _unloadQueue.Remove(loaded);
                _unloadQueue.AddLast(loaded);
            }
        }

        Task<byte[]> IBlobMapInternal<T>.Fetch(long id) => _storage.Fetch(FormatId(id));
        
        private void CheckSavingTasks()
        {
            var completed = _savingTasks.Where(t => t.IsCompleted).ToList();

            var errors = completed.Where(t => t.Exception != null)
                                  .Select(t => t.Exception)
                                  .ToList();

            if (errors.Count != 0) throw new AggregateException(errors);

            _savingTasks = _savingTasks.RemoveAll(t => completed.Contains(t));
        }

        async Task IBlobMapInternal<T>.FlushPendingSaves(int maxCount)
        {
            while (_savingTasks.Count > maxCount)
            {
                await Task.WhenAny(_savingTasks);

                CheckSavingTasks();
            }
        }

        async Task IBlobMapInternal<T>.Store(long id, byte[] data)
        {
            CheckSavingTasks();

            IBlobMapInternal<T> internalThis = this;
            await internalThis.FlushPendingSaves(20);

            _savingTasks = _savingTasks.Add(_storage.Store(FormatId(id), data));
        }
    }
}
