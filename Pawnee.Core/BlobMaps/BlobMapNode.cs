namespace Pawnee.Core.BlobMaps
{
    using System.IO;
    using System.Threading.Tasks;
    using MessagePack;

    public interface IReadOnlyBlobMapKey<out T>
    {
        bool HasValue { get; }

        string Key { get; }

        T GetValue();
    }

    public interface IBlobMapKey<T> : IReadOnlyBlobMapKey<T>
    {
        Task UpdateValue(T value);

        Task UpdateRawValue(byte[] bytes);
    }

    public class BlobMapStats
    {
        public int LeafKeysLoaded;
        public int LeafNodesLoaded;
        public int LeafNodesUnloaded;
        public int BranchNodesLoaded;
        public int BranchNodesUnloaded;
    }

    public interface IBlobMapNode<T>
    {
        long Id { get; }

        void SetDirty();

        bool IsLoaded { get; }

        Task Load();

        Task Save();

        Task<bool> UnloadIfPossible();

        Task<IBlobMapKey<T>> GetKey(string key);

        Task<IReadOnlyBlobMapKey<T>> GetReadOnlyKey(string key);

        Task<bool> DeleteKey(string key);
        
        void ReplaceParent(IBlobMapBranch<T> parent);

        void GatherStats(BlobMapStats stats);

        void PrintTree(TextWriter output, int depth = 0);
    }

    public abstract class BlobMapNode<T> : IBlobMapNode<T>
    {
        protected readonly IBlobMapInternal<T> Tree;

        protected IBlobMapBranch<T> Parent { get; private set; }

        [IgnoreMember]
        public long Id { get; private set; }

        [IgnoreMember]
        public bool IsDirty { get; protected set; }

        public void SetDirty()
        {
            if (!IsDirty)
            {
                IsDirty = true;

                if (Parent != null)
                {
                    ChangeId();
                    Parent.SetDirty();
                }
            }
        }

        public void ChangeId()
        {
            Tree.AddGarbage(Id);
            Id = Tree.AllocateId();
        }

        public void OnCreate()
        {
            IsDirty = true;
        }

        public abstract bool IsLoaded { get; }

        public abstract Task Load();

        public abstract Task Save();
        
        public abstract Task<IBlobMapKey<T>> GetKey(string key);

        public abstract Task<IReadOnlyBlobMapKey<T>> GetReadOnlyKey(string key);

        public BlobMapNode(IBlobMapInternal<T> manager, IBlobMapBranch<T> parent, long id)
        {
            Tree = manager;
            Parent = parent;

            Id = id;
        }
        
        public void ReplaceParent(IBlobMapBranch<T> parent)
        {
            Parent = parent;
        }

        public abstract void GatherStats(BlobMapStats stats);

        public abstract void PrintTree(TextWriter output, int depth = 0);

        public abstract Task<bool> UnloadIfPossible();

        public abstract Task<bool> DeleteKey(string key);
    }
}
