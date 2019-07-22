namespace Pawnee.Core.BlobMaps
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Batched;

    public class Cursor<T> : IBatchedEnumerator<(string key, T value)>
    {
        public class BranchCursor
        {
            public BlobMapBranch<T> Branch;
            public int Position;
        }

        private readonly List<BranchCursor> _stack;
        private readonly Func<string, bool> _takeWhile;
        private string _minKey;
        
        public IEnumerable<(string key, T value)> CurrentBatch { get; private set; }

        public Cursor(BlobMapBranch<T> root, string minKey, Func<string, bool> takeWhile = null)
        {
            _stack = new List<BranchCursor>
            {
                new BranchCursor { Branch = root, Position = -1 }
            };

            _minKey = minKey;
            _takeWhile = takeWhile;
        }

        public async Task<bool> GetNextBatch(int _)
        {
            BlobMapLeaf<T> leaf = null;

            if (_stack[0].Position == -1)
            {
                while(true)
                {
                    var top = _stack[_stack.Count - 1];
                    if (!top.Branch.IsLoaded) await top.Branch.Load();

                    top.Position = top.Branch.GetLowerBoundIndex(_minKey);

                    var keys = top.Branch.GetSortedKeys();
                    var next = top.Branch.ChildNodesByKey[keys[top.Position]];

                    leaf = next as BlobMapLeaf<T>;
                    if (leaf != null) break;

                    _stack.Add(new BranchCursor { Branch = (BlobMapBranch<T>)next });
                };
            }
            else
            {
                var top = _stack[_stack.Count - 1];
                if (!top.Branch.IsLoaded) await top.Branch.Load();

                var keys = top.Branch.GetSortedKeys();
                if (top.Position >= keys.Count)
                {
                    if (!await IncrementStackLevel(_stack.Count - 2)) return false;
                    keys = top.Branch.GetSortedKeys();
                }

                leaf = (BlobMapLeaf<T>)top.Branch.ChildNodesByKey[keys[top.Position]];
            }

            if (!leaf.IsLoaded) await leaf.Load();

            var leafKeys = leaf.GetSortedKeys();

            var (leafPosition, _) = leafKeys.BinarySearch(_minKey, StringComparer.OrdinalIgnoreCase);

            if (_takeWhile != null &&
                leafPosition < leafKeys.Count &&
                !_takeWhile(leafKeys[leafPosition]))
            {
                return false;
            }

            var range = leafKeys.Skip(leafPosition);
            if (_takeWhile != null) range = range.TakeWhile(_takeWhile);

            CurrentBatch = range.Select(k => (k, BlobMapLeaf<T>.Deserialize(leaf.ValuesByKey[k])));
               
            _stack[_stack.Count - 1].Position++;
            _minKey = string.Empty;
            return true;
        }

        private async Task<bool> IncrementStackLevel(int level)
        {
            if (level < 0) return false;

            var top = _stack[level];
            var keys = top.Branch.GetSortedKeys();

            top.Position++;

            if (top.Position >= keys.Count)
            {
                if (!await IncrementStackLevel(level - 1)) return false;

                top = _stack[level];
                keys = top.Branch.GetSortedKeys();
            }

            var next = _stack[level + 1];
            next.Branch = (BlobMapBranch<T>)top.Branch.ChildNodesByKey[keys[top.Position]];
            next.Position = 0;

            if (!next.Branch.IsLoaded) await next.Branch.Load();            
            return true;
        }

        public void Dispose() { }
    }
}
