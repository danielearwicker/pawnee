namespace Pawnee.Core.BlobMaps
{
    public interface IBlobMapFactory
    {
        IBlobMap<T> Create<T>(IBlobStorage storage, BlobMapOptions options);
    }

    public class BlobMapFactory : IBlobMapFactory
    {
        private readonly ITimings _timings;

        public BlobMapFactory(ITimings timings) => _timings = timings;

        public IBlobMap<T> Create<T>(IBlobStorage storage, BlobMapOptions options)
            => new BlobMapTree<T>(storage, _timings, options);
    }
}
