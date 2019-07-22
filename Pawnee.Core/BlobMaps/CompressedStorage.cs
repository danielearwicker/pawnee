namespace Pawnee.Core.BlobMaps
{
    using System.IO;
    using System.IO.Compression;
    using System.Threading.Tasks;

    public class CompressedStorage : IBlobStorage
    {
        private readonly IBlobStorage _impl;

        public CompressedStorage(IBlobStorage impl) => _impl = impl;

        public Task Clear() => _impl.Clear();

        public Task Delete(string key) => _impl.Delete(key);

        public async Task<byte[]> Fetch(string key)
        {
            var bytes = await _impl.Fetch(key);

            if (bytes == null) return null;

            var decompressed = new MemoryStream();

            using (var gzip = new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress))
            {
                gzip.CopyTo(decompressed);
            }

            return decompressed.ToArray();
        }

        public async Task<Stream> Read(string key)
        {
            var blob = await _impl.Read(key);
            return blob == null ? null : new GZipStream(blob, CompressionMode.Decompress);
        }

        public Task Store(string key, byte[] data)
        {
            var stream = new MemoryStream();
            using (var zip = new GZipStream(stream, CompressionLevel.Fastest))
            {
                zip.Write(data, 0, data.Length);
            }
            return _impl.Store(key, stream.ToArray());
        }
    }
}
