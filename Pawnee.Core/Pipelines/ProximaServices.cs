using BlobMap;
using Pawnee.Core.Queue;
using System;

namespace Pawnee.Core
{
    public interface IPawneeScaleParameters
    {
        int TrackingReadBatchSize { get; }
        int ChangesMergeBatchSize { get; }
        int ChangesReadBatchSize { get; }
    }

    public class PawneeScaleParameters : IPawneeScaleParameters
    {
        public int TrackingReadBatchSize { get; set; } = 10000;

        public int ChangesMergeBatchSize { get; set; } = 10000;

        public int ChangesReadBatchSize { get; set; } = 10000;
    }

    public interface IPawneeServices
    {
        Func<string, IBlobStorage> Storage { get; }
        IChunkProcessorFactory Processors { get; }
        IPawneeMapFactory Maps { get; }
        IChangeListStorage ChangeLists { get; }
        IPawneeQueueStorage Queues { get; }
        IPawneeQueueClient QueueClient { get; }
        IPawneeScaleParameters ScaleParameters { get; }
        IStatusEvents Log { get; }
    }

    public class PawneeServices : IPawneeServices
    {
        public Func<string, IBlobStorage> Storage { get; }
        public IChunkProcessorFactory Processors { get; }
        public IPawneeMapFactory Maps { get; }
        public IChangeListStorage ChangeLists { get; }
        public IPawneeQueueStorage Queues { get; }
        public IPawneeQueueClient QueueClient { get; }
        public IPawneeScaleParameters ScaleParameters { get; }
        public IStatusEvents Log { get; }

        public PawneeServices(Func<string, IBlobStorage> storage,
                               IChunkProcessorFactory processors,
                               IPawneeMapFactory maps,
                               IChangeListStorage changeLists,
                               IPawneeQueueStorage queues,
                               IPawneeQueueClient queueClient,
                               IPawneeScaleParameters scaleParameters,
                               IStatusEvents log)
        {
            Storage = storage;
            Processors = processors;
            Maps = maps;
            ChangeLists = changeLists;
            Queues = queues;
            QueueClient = queueClient;
            ScaleParameters = scaleParameters;
            Log = log;
        }
    }
}
