using StackExchange.Redis;

namespace Pawnee.Core
{
    public interface IRedisConnection
    {
        IDatabase Database { get; }
    }

    public class RedisConnection : IRedisConnection
    {
        private readonly ConnectionMultiplexer _redis;

        public RedisConnection(string connectionString)        
            => _redis = ConnectionMultiplexer.Connect(connectionString);
        
        public IDatabase Database => _redis.GetDatabase();
    }
}
