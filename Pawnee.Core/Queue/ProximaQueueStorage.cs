using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Pawnee.Core.Queue
{
    public interface IPawneeQueueStorage
    {
        Task<IPawneeQueueState> Read();

        Task<bool> Commit(IPawneeQueueState state);        
    }

    public class PawneeQueueStorage : IPawneeQueueStorage
    {
        private readonly IRedisConnection _redis;
        private readonly IStatusEvents _statusEvents;

        private PawneeQueueState _currentState;
        private bool _triedInitializing;

        public PawneeQueueStorage(IRedisConnection redis,
                                   IStatusEvents statusEvents)
        {
            _redis = redis;
            _statusEvents = statusEvents;
        }

        private const string QueueVersion = nameof(Pawnee) + nameof(QueueVersion);
        private const string QueueState = nameof(Pawnee) + nameof(QueueState);
        private const string QueueMessages = nameof(Pawnee) + nameof(QueueMessages);

        public async Task<IPawneeQueueState> Read()
        {
            if (_currentState != null)
            {
                var result = await _redis.Database.StringGetAsync(QueueVersion);
                if (result.HasValue &&
                    int.TryParse(result.ToString(), out var version) &&
                    _currentState.Version == version)
                {
                    return _currentState;
                }

                _currentState = null;
            }

            if (_currentState == null)
            {
                var result = await _redis.Database.StringGetAsync(new RedisKey[] { QueueVersion, QueueState });

                if (!result[0].IsNullOrEmpty && !result[1].IsNullOrEmpty)
                {
                    _currentState = new PawneeQueueState(this, result[0], result[1]);
                }
                else
                {
                    _currentState = new PawneeQueueState(this);
                }
            }

            return new PawneeQueueState(_currentState);
        }

        public async Task<bool> Commit(IPawneeQueueState state)
        {
            if (!(state is PawneeQueueState internalState))
                throw new ArgumentOutOfRangeException(nameof(state));

            if (!internalState.IsDirty) return true;

            var oldVersion = internalState.Version.ToString();

            internalState.Version++;

            var updates = new[]
            {
                new KeyValuePair<RedisKey, RedisValue>(QueueVersion, internalState.Version.ToString()),
                new KeyValuePair<RedisKey, RedisValue>(QueueState, internalState.State)
            };

            var transaction = _redis.Database.CreateTransaction();
            transaction.AddCondition(Condition.StringEqual(QueueVersion, oldVersion));
            _ = transaction.StringSetAsync(updates);

            if (await transaction.ExecuteAsync())
            {
                _currentState = internalState;
                await _statusEvents.QueueUpdated(internalState.Version, state.Items);                
                return true;
            }

            if (!_triedInitializing)
            {
                _triedInitializing = true;

                transaction = _redis.Database.CreateTransaction();
                transaction.AddCondition(Condition.KeyNotExists(QueueVersion));
                _ = transaction.StringSetAsync(updates);

                if (await transaction.ExecuteAsync())
                {
                    _currentState = internalState;
                    await _statusEvents.QueueUpdated(internalState.Version, state.Items);
                    return true;
                }
            }

            _currentState = null;
            return false;
        }
    }
}
