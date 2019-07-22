using BlobMap;
using Platform;
using Pawnee.Core.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Pawnee.Core
{
    public class ProjectionGroup<TIn, TOut>
    {
        private readonly Func<string, TOut, Task> _output;

        public string Key { get; }

        public IBatchedEnumerator<TIn> Inputs { get; }

        public Task Output(string group, TOut output) => _output(group, output);

        public ProjectionGroup(
            string key,
            IBatchedEnumerator<TIn> inputs,
            Func<string, TOut, Task> output)
        {
            Key = key;
            Inputs = inputs;
            _output = output;
        }
    }


    public class PawneeProjection<TIn, TOut> : PawneeStage, IPawneeOutput, IPawneeStage<TOut>
    {
        private readonly IReadOnlyList<IPawneeStage<TIn>> _inputs;
        private Func<ProjectionGroup<TIn, TOut>, Task> _projector;

        public PawneeProjection(
            IPawneeServices services,
            string name,
            IReadOnlyList<IPawneeStage<TIn>> inputs) 
            : base(services, name)
        {            
            _inputs = inputs;

            foreach (var input in inputs)
            {
                input.AddOutput(this);
            }
        }

        public IBlobMultiMap<TOut> GetData() => Services.Maps.OpenData<TOut>(Name, true);

        public IPawneeStage<TOut> Map(Func<ProjectionGroup<TIn, TOut>, Task> projector)
        {
            _projector = projector;
            return this;
        }

        protected override bool IsSuspended(IPawneeQueueState queue)
            => queue.Items.Any(j => _inputs.Any(i => i.Name == j.Stage));
       
        public void InputHasCompleted(string fromStage, IPawneeQueueState queue, int chunkCount)
            => Services.QueueClient.Enqueue<IPawneeOutput>(
                queue,
                Name,
                p => p.PrepareInputs(chunkCount),
                fromStage: fromStage);
        
        public async Task PrepareInputs(int chunkCount)
        {
            // If we have multiple inputs, merge their change chunks into
            // a new ordered set for our use.
            if (_inputs.Count > 1)
            {
                var inputLists = _inputs.Select(i => Services.ChangeLists.Enumerate(i.Name))
                                        .Merge(StringComparer.OrdinalIgnoreCase.Compare);

                var saver = Services.ChangeLists.Save(Name);

                while (await inputLists.GetNextBatch(Services.ScaleParameters.ChangesMergeBatchSize))
                {
                    foreach (var change in inputLists.CurrentBatch)
                    {                        
                        if (saver.Add(change)) await saver.Flush();
                    }
                }

                chunkCount = await saver.Complete();
            }

            await Services.Queues.InTransaction(queue =>
            {
                var chunkJobs = Guid.NewGuid();

                for (int n = 0; n < chunkCount; n++)
                {
                    Services.QueueClient.Enqueue<IPawneeOutput>(
                        queue, Name, 
                        p => p.ProcessChunk(n, chunkCount),
                        chunkJobs);
                }

                EnqueueMergeJobs(queue, chunkCount, chunkJobs);
            });
        }

        public async Task ProcessChunk(int chunkId, int chunkCount)
        {
            var changes = await Services.ChangeLists.Load(_inputs.Count > 1 ? Name : _inputs[0].Name, chunkId);

            var inputMaps = _inputs.Select(i => i.GetData()).ToList();

            var processor = Services.Processors.Create<TOut>(new ChunkProcessorOptions
            {
                Stage = Name,
                ChunkId = chunkId,
                ChunkCount = chunkCount
            });

            await processor.Initialize();

            foreach (var change in changes)
            {
                await processor.DeleteGroup(change);

                var records = inputMaps.Select(i => i.IterateKey(change))
                                        .Concat()
                                        .Select(r => r.Value);

                await _projector(new ProjectionGroup<TIn, TOut>(change, records, (group, record) =>
                    processor.AddGroup(group, change, record)));
            }

            await Services.Log.ProgressLogged(new ProgressUpdate
            {
                Stage = Name,
                Aspect = "generating",
                Instance = chunkId,
                InstanceCount = chunkCount,
                Message = "Saving results..."
            });

            await processor.Save();

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

        public Task MergeData(int batchCount, Guid mergeJobBatch) => MergeData<TOut>(batchCount, mergeJobBatch);

        public void GetUpstreamJobs(IPawneeQueueState queue, Action<IPawneeQueueItem> add)
        {
            foreach (var input in _inputs)
            {
                foreach (var job in queue.Items.Where(j => j.Stage == input.Name))
                {
                    add(job);
                }

                if (input is IPawneeOutput output)
                {
                    output.GetUpstreamJobs(queue, add);
                }
            }
        }
    }
}
