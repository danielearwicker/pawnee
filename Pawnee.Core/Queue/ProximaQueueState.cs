using MessagePack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Pawnee.Core.Queue
{
    public interface IPawneeQueueState
    {
        IReadOnlyList<IPawneeQueueItem> Items { get; }

        int Version { get; set; }

        IPawneeQueueItem Enqueue(
            string stage,
            string content,
            Guid? batch = null,
            Guid? afterBatch = null,
            string fromStage = null);

        IPawneeQueueItem Dequeue(string stage, string workerId);

        void Ping(IPawneeQueueItem item);

        void Release(IPawneeQueueItem item, string error = null);

        Task<bool> Commit();
    }

    public class PawneeQueueState : IPawneeQueueState
    {
        private readonly IPawneeQueueStorage _owner;
        private readonly List<PawneeQueueItem> _items;

        public bool IsDirty { get; set; }

        public int Version { get; set; }

        public byte[] State => LZ4MessagePackSerializer.Serialize(_items);

        public PawneeQueueState(IPawneeQueueStorage owner, string version = null, byte[] state = null)
        {
            _owner = owner;
            _items = new List<PawneeQueueItem>();

            if (version != null && state != null)
            {
                Version = int.Parse(version);
                _items = LZ4MessagePackSerializer.Deserialize<List<PawneeQueueItem>>(state);
            }
        }

        public PawneeQueueState(PawneeQueueState cloneFrom)
        {
            _owner = cloneFrom._owner;
            _items = cloneFrom._items.ToList();
            Version = cloneFrom.Version;
        }

        public IReadOnlyList<IPawneeQueueItem> Items => _items;

        public Task<bool> Commit()
        {
            return _owner.Commit(this);
        }

        public IPawneeQueueItem Dequeue(string stage, string workerId)
        {
            var expiryAge = DateTime.UtcNow - TimeSpan.FromMinutes(1);

            var batches = _items.ToLookup(i => i.Batch);

            var item = _items.FirstOrDefault(i => i.Stage == stage &&
                            (i.AfterBatch == null || !batches[i.AfterBatch.Value].Any()) &&
                            (i.Claimed == null || i.Claimed < expiryAge));

            if (item != null)
            {
                item.Started = item.Claimed = DateTime.UtcNow;
                item.Worker = workerId;
                IsDirty = true;
            }
            return item;
        }

        public IPawneeQueueItem Enqueue(
            string stage,
            string content,
            Guid? batch = null,
            Guid? afterBatch = null,
            string fromStage = null)
        {
            var item = new PawneeQueueItem
            {
                AfterBatch = afterBatch,
                Batch = batch ?? Guid.NewGuid(),
                Content = content,
                Enqueued = DateTime.UtcNow,
                From = fromStage,
                Stage = stage,
                Id = Guid.NewGuid()
            };

            IsDirty = true;
            _items.Add(item);
            return item;
        }

        private PawneeQueueItem FindItem(IPawneeQueueItem item)
            => _items.FirstOrDefault(i => i.Id == item.Id);

        public void Ping(IPawneeQueueItem item)
        {
            var internalItem = FindItem(item);
            if (internalItem == null) return;
            
            IsDirty = true;
            internalItem.Claimed = DateTime.UtcNow;            
        }

        public void Release(IPawneeQueueItem item, string error = null)
        {
            var internalItem = FindItem(item);
            if (internalItem == null) return;

            IsDirty = true;
            
            if (error != null)
            {
                internalItem.Error = error;
                internalItem.FailureCount++;
                internalItem.Claimed = null;
                internalItem.Worker = null;
            }
            else
            {
                _items.Remove(internalItem);
            }
        }
    }
}
