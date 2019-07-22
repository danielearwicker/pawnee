using System;
using System.Threading.Tasks;

namespace Pawnee.Core
{
    public class ChunkProcessorOptions
    {
        public string Stage { get; set; }
        public int ChunkId { get; set; }
        public int ChunkCount { get; set; }
    }

    public interface IChunkProcessor<TOut>
    {
        Task Initialize();

        Task DeleteGroup(string sourceGroup);

        Task AddGroup(string targetGroup, string sourceGroup, TOut record);

        Task Save();
    }

    public class ChunkProcessor<TOut> : IChunkProcessor<TOut>
    {
        private readonly IBlobMultiMap<Command> _trackingCommands;
        private readonly IBlobMultiMap<Command> _dataCommands;        
        private readonly IBlobMultiMap<MultiMapKey> _tracking;
        private readonly IPawneeScaleParameters _scaleParameters;
        private readonly IStatusEvents _log;
        private readonly RateLogger _rate;

        public ChunkProcessor(
            IPawneeMapFactory maps,
            IPawneeScaleParameters scaleParameters,
            IStatusEvents log,
            ChunkProcessorOptions options)
        {
            _log = log;
            _rate = new RateLogger(_log, options.Stage, "generating", 
                                   options.ChunkId, options.ChunkCount, 
                                   TimeSpan.FromSeconds(5));

            _scaleParameters = scaleParameters;

            _trackingCommands = maps.OpenTrackingCommands(options.Stage, options.ChunkId, true);            
            _dataCommands = maps.OpenDataCommands(options.Stage, options.ChunkId, true);            
            _tracking = maps.OpenTracking(options.Stage, true);
        }

        public Task Initialize() => Task.WhenAll(_trackingCommands.Clear(),
                                                _dataCommands.Clear());
        
        public Task Save() => Task.WhenAll(_dataCommands.SaveOverwrite(),
                                           _trackingCommands.SaveOverwrite());

        public async Task DeleteGroup(string sourceGroup)
        {
            var trackingForGroup = _tracking.IterateKey(sourceGroup);

            while (await trackingForGroup.GetNextBatch(_scaleParameters.TrackingReadBatchSize))
            {
                foreach (var (trackingToDelete, dataToDelete) in trackingForGroup.CurrentBatch)                            
                {
                    await _dataCommands.Upsert(dataToDelete, new Command());
                    await _trackingCommands.Upsert(trackingToDelete, new Command());
                }
            }
        }

        public async Task AddGroup(string targetGroup, string sourceGroup, TOut record)
        {
            _rate.Increment(targetGroup);

            var dataKey = await _dataCommands.Add(targetGroup, Command.FromRecord(record));
            await _trackingCommands.Add(sourceGroup, Command.FromRecord(dataKey));
        }
    }

    public interface IChunkProcessorFactory
    {
        IChunkProcessor<T> Create<T>(ChunkProcessorOptions options);
    }

    public class ChunkProcessorFactory : IChunkProcessorFactory
    {
        private readonly IPawneeMapFactory _maps;
        private readonly IPawneeScaleParameters _scaleParameters;
        private readonly IStatusEvents _log;

        public ChunkProcessorFactory(
            IPawneeMapFactory maps,
            IPawneeScaleParameters scaleParameters,
            IStatusEvents log)
        {
            _maps = maps;
            _scaleParameters = scaleParameters;
            _log = log;
        }
        
        public IChunkProcessor<T> Create<T>(ChunkProcessorOptions options)
            => new ChunkProcessor<T>(_maps, _scaleParameters, _log, options);
    }
}
