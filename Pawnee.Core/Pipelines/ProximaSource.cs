namespace Pawnee.Core.Pipelines
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using BlobMaps;
    using Queue;

    public interface IPawneeSource : IPawneeStage
    {
        Task PrepareInputs(string prefix, int chunkCount);

        Task ProcessChunk(string prefix, int chunkId, int chunkCount);
    }

    public class PawneeSource<TOut> : PawneeStage, IPawneeSource, IPawneeStage<TOut>
    {
        private readonly Func<Stream, Func<string, TOut, Task>, Task> _parser;

        public PawneeSource(
            IPawneeServices services,
            string name,
            Func<Stream, Func<string, TOut, Task>, Task> parser) : base(services, name)
        {
            _parser = parser;
        }

        public IBlobMultiMap<TOut> GetData() => Services.Maps.OpenData<TOut>(Name, true);

        public async Task PrepareInputs(string prefix, int chunkCount)
        {
            await Services.Queues.InTransaction(queue =>
            {
                var chunkJobs = Guid.NewGuid();

                for (int n = 0; n < chunkCount; n++)
                {
                    Services.QueueClient.Enqueue<IPawneeSource>(
                        queue, Name,
                        p => p.ProcessChunk(prefix, n, chunkCount),
                        chunkJobs);
                }

                EnqueueMergeJobs(queue, chunkCount, chunkJobs);
            });
        }

        public async Task ProcessChunk(string prefix, int chunkId, int chunkCount)
        {
            var uploads = new CompressedStorage(Services.Storage($"{Name}-uploads"));

            using (var stream = await uploads.Read($"{prefix}-{chunkId}"))
            {
                var processor = Services.Processors.Create<TOut>(new ChunkProcessorOptions
                {
                    Stage = Name,
                    ChunkId = chunkId,
                    ChunkCount = chunkCount
                });

                await processor.Initialize();

                await processor.DeleteGroup(prefix);

                await _parser(stream, (groupId, record) => processor.AddGroup(groupId, prefix, record));

                await Services.Log.ProgressLogged(new ProgressUpdate
                {
                    Stage = Name,
                    Aspect = "generating",
                    Instance = chunkId,
                    InstanceCount = chunkCount,
                    Message = "Saving results..."
                });

                await processor.Save();
            }

            await Services.Log.ProgressLogged(new ProgressUpdate
            {
                Stage = Name,
                Aspect = "generating",
                Instance = chunkId,
                InstanceCount = chunkCount,
                Message = "Finished",
                Status = ProgressStatus.Succeeded
            });
        }

        public Task MergeData(int chunkCount, Guid mergeJobBatch) => MergeData<TOut>(chunkCount, mergeJobBatch);

        protected override bool IsSuspended(IPawneeQueueState queue) => false;
    }
}
