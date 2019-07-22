namespace Pawnee.Core.Pipelines
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Batched;
    using BlobMaps;
    using MessagePack;

    public interface IChangeListStorage
    {
        Task<IReadOnlyCollection<string>> Load(string stage, int id);

        Task Save(string stage, int id, IReadOnlyCollection<string> list);

        Task Delete(string stage, int id);

        IBatchedEnumerator<string> Enumerate(string stage);

        IChangeListSaver Save(string stage);
    }

    public interface IChangeListSaver
    {
        bool Add(string change);

        Task Flush();

        Task<int> Complete();
    }

    public class ChangeListSaver : IChangeListSaver
    {
        private readonly IChangeListStorage _storage;
        private readonly string _stage;
        private readonly List<string> _chunk = new List<string>();

        private int _nextId;

        public ChangeListSaver(IChangeListStorage storage, string stage)
        {
            _storage = storage;
            _stage = stage;
        }

        public bool Add(string change)
        {
            _chunk.Add(change);
            return _chunk.Count > 500000;
        }

        public async Task Flush()
        {
            if (_chunk.Count > 0)
            {
                await _storage.Save(_stage, _nextId, _chunk);

                _chunk.Clear();
                _nextId++;
            }
        }

        public async Task<int> Complete()
        {
            await Flush();            
            await _storage.Delete(_stage, _nextId);
            return _nextId;
        }
    }

    public class ChangeListEnumerator : IBatchedEnumerator<string>
    {
        private readonly IChangeListStorage _storage;
        private readonly string _stage;

        private int _nextId;

        public ChangeListEnumerator(IChangeListStorage storage, string stage)
        {
            _storage = storage;
            _stage = stage;
        }

        public IEnumerable<string> CurrentBatch { get; private set; }

        public void Dispose() { }

        public async Task<bool> GetNextBatch(int _)
        {
            var loaded = await _storage.Load(_stage, _nextId);
            if (loaded == null)
            {
                CurrentBatch = Enumerable.Empty<string>();
                return false;
            }

            _nextId++;
            CurrentBatch = loaded;
            return true;
        }
    }

    public class ChangeListStorage : IChangeListStorage
    {
        private readonly Func<string, IBlobStorage> _storage;

        public ChangeListStorage(Func<string, IBlobStorage> storage) => _storage = storage;
        
        public IBatchedEnumerator<string> Enumerate(string stage)
            => new ChangeListEnumerator(this, stage);

        public IChangeListSaver Save(string stage)
            => new ChangeListSaver(this, stage);

        private IBlobStorage GetStorage(string stage)
            => new CompressedStorage(_storage($"{stage}-changes"));

        public async Task<IReadOnlyCollection<string>> Load(string stage, int id)
        {
            var blob = await GetStorage(stage).Fetch($"{id}");
            return blob == null ? null : MessagePackSerializer.Deserialize<List<string>>(blob);
        }

        public Task Save(string stage, int id, IReadOnlyCollection<string> list)
            => GetStorage(stage).Store($"{id}", MessagePackSerializer.Serialize(list));

        public Task Delete(string stage, int id)
            => GetStorage(stage).Delete($"{id}");
    }
}
