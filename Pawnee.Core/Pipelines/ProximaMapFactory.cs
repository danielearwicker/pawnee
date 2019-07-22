using System;
using BlobMap;
using MessagePack;
using Microsoft.Extensions.Logging;

namespace Pawnee.Core
{
    [MessagePackObject]
    public class Command
    {
        [Key(0)]
        public byte[] Upsert { get; set; }

        public static Command FromRecord<T>(T record)
            => new Command { Upsert = MessagePackSerializer.Serialize(record) };
    }

    public interface IPawneeMapFactory
    {
        IBlobMultiMap<Command> OpenDataCommands(string stage, int chunkId, bool randomAccess);

        IBlobMultiMap<T> OpenData<T>(string stage, bool randomAccess);

        IBlobMultiMap<Command> OpenTrackingCommands(string stage, int chunkId, bool randomAccess);

        IBlobMultiMap<MultiMapKey> OpenTracking(string stage, bool randomAccess);
    }

    public class PawneeMapFactory : IPawneeMapFactory
    {
        private readonly Func<string, IBlobStorage> _storage;
        private readonly IBlobMapFactory _maps;
        private readonly ILogger _logger;

        public PawneeMapFactory(Func<string, IBlobStorage> storage, 
                          IBlobMapFactory maps, 
                          ILogger<PawneeMapFactory> logger)
        {
            _storage = storage;
            _maps = maps;
            _logger = logger;
        }

        private IBlobMultiMap<T> Open<T>(string stage, string name, int size, bool randomAccess)
        {
            name = $"v35-{stage}-{name}";
            
            return new BlobMultiMap<T>(_maps.Create<T>(
                new CompressedStorage(
                    new LoggingBlobStorage(_storage(name), _logger, $"{stage}:{name}")),
                new BlobMapOptions
                {
                    MaxNodeKeys = size,
                    MaxLeavesLoaded = randomAccess ? 500 : 5,
                    Name = name
                }));
        }

        private IBlobMultiMap<T> OpenCommands<T>(string type, string stage, int chunkId, bool randomAccess)
            => Open<T>(stage, $"commands-{type}-{chunkId:0000}", 10000, randomAccess);
        
        public IBlobMultiMap<Command> OpenDataCommands(string stage, int chunkId, bool randomAccess)
            => OpenCommands<Command>("data", stage, chunkId, randomAccess);

        public IBlobMultiMap<Command> OpenTrackingCommands(string stage, int chunkId, bool randomAccess)
            => OpenCommands<Command>("tracking", stage, chunkId, randomAccess);

        public IBlobMultiMap<MultiMapKey> OpenTracking(string stage, bool randomAccess)
            => Open<MultiMapKey>(stage, "tracking", 20000, randomAccess);

        public IBlobMultiMap<T> OpenData<T>(string stage, bool randomAccess)
            => Open<T>(stage, "data", 10000, randomAccess);
    }
}
