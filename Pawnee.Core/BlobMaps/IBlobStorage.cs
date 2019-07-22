namespace Pawnee.Core.BlobMaps
{
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    public interface IBlobStorage
    {
        Task<Stream> Read(string key);

        Task<byte[]> Fetch(string key);
        Task Store(string key, byte[] data);
        Task Delete(string key);

        Task Clear();
    }

    public class LoggingBlobStorage : IBlobStorage
    {
        private readonly IBlobStorage _impl;
        private readonly ILogger _log;
        private readonly string _name;

        private readonly Dictionary<string, int> _fetchCounts = new Dictionary<string, int>();

        private void Count(string key)
        {
            int count;

            lock (_fetchCounts)
            {
                _fetchCounts.TryGetValue(key, out count);
                count++;
                _fetchCounts[key] = count;
            }

            if (count > 1)
            {
                _log.LogInformation("In {name}, key {key} has now been read or written {count} times", _name, key, count);
            }
        }

        public LoggingBlobStorage(IBlobStorage impl, ILogger log, string name)
        {
            _impl = impl;
            _log = log;
            _name = name;
        }

        public Task Delete(string key)
        {
            return _impl.Delete(key);
        }

        public Task<byte[]> Fetch(string key)
        {
            Count(key);

            return _impl.Fetch(key);
        }

        public Task<Stream> Read(string key)
        {
            return _impl.Read(key);
        }

        public Task Store(string key, byte[] data)
        {
            Count(key);

            return _impl.Store(key, data);
        }

        public Task Clear()
        {
            return _impl.Clear();
        }
    }
}
