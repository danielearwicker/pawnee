using Platform;
using Pawnee.Core.Queue;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Pawnee.Core
{
    public interface IPawneeStage<TOut>
    {
        string Name { get; }

        void AddOutput(IPawneeOutput output);

        IBlobMultiMap<TOut> GetData();
    }

    public interface IPawneeStage
    {
        string Name { get; }
        object Description { get; }

        IPawneeQueueItem ClaimJob(IPawneeQueueState queue, string workerId);
        Task Execute(IPawneeQueueItem job);

        Task MergeData(int chunkCount, Guid mergeJobBatch);
        Task MergeTracking(int chunkCount);
        Task NotifyOutputs(int chunkCount);
    }

    public interface IPawneeOutput : IPawneeStage
    {
        void InputHasCompleted(string input, IPawneeQueueState queue, int chunkCount);

        Task PrepareInputs(int chunkCount);

        Task ProcessChunk(int chunkId, int chunkCount);

        void GetUpstreamJobs(IPawneeQueueState queue, Action<IPawneeQueueItem> add);
    }

    public abstract class PawneeStage
    {
        private readonly List<IPawneeOutput> _outputs = new List<IPawneeOutput>();

        public IPawneeServices Services { get; }

        public string Name { get; }

        public object Description => new { Name, Outputs = _outputs.Select(o => o.Name) };

        protected PawneeStage(IPawneeServices services, string name)
        {
            Services = services;
            Name = name;
        }

        public void AddOutput(IPawneeOutput output) => _outputs.Add(output);

        public Task NotifyOutputs(int chunkCount) 
            => Services.Queues.InTransaction(queue =>
            {
                foreach (var output in _outputs)
                {
                    var otherUpstreamJobs = new List<IPawneeQueueItem>();
                    output.GetUpstreamJobs(queue, job =>
                    {
                        if (job.Stage != Name) otherUpstreamJobs.Add(job);
                    });

                    if (otherUpstreamJobs.Count == 0)
                    {
                        output.InputHasCompleted(Name, queue, chunkCount);
                    }
                    else
                    {
                        var otherStages = string.Join(", ", otherUpstreamJobs.Select(j => j.Stage).Distinct());

                        Services.Log.ProgressLogged(new ProgressUpdate
                        {
                            Stage = Name,
                            Aspect = "notify-outputs",
                            Message = $"Will not notify {output.Name} as it has other upstream jobs in {otherStages}"
                        });
                    }
                }
            });

        protected abstract bool IsSuspended(IPawneeQueueState queue);

        public IPawneeQueueItem ClaimJob(IPawneeQueueState queue, string workerId)
        {
            var outputsBusy = queue.Items.Any(i =>
                    _outputs.Any(o => i.Stage == o.Name &&
                                      (i.From == null || i.From == Name)));
            if (outputsBusy || IsSuspended(queue)) return null;

            return queue.Dequeue(Name, workerId);
        }

        public async Task Execute(IPawneeQueueItem job)
        {
            var visibilityExpiring = Task.Delay(TimeSpan.FromSeconds(20));
            var handling = Services.QueueClient.Invoke(job, this);

            do
            {
                var completed = await Task.WhenAny(handling, visibilityExpiring);
                if (completed == visibilityExpiring)
                {
                    await Services.Queues.InTransaction(queue => queue.Ping(job));
                    visibilityExpiring = Task.Delay(TimeSpan.FromSeconds(20));
                }
            }
            while (!handling.IsCompleted);

            if (handling.Exception != null)
            {
                var error = handling.Exception.GetBaseException().Message;
                await Services.Log.ProgressLogged(new ProgressUpdate
                {
                    Stage = job.Stage,
                    Message = error,
                    Status = ProgressStatus.Failed
                });

                await Services.Queues.InTransaction(queue => queue.Release(job, error));
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
            else
            {
                await Services.Queues.InTransaction(queue => queue.Release(job));
            }
        }

        public void EnqueueMergeJobs(IPawneeQueueState queue, int chunkCount, Guid chunkJobs)
        {
            var mergeJobs = Guid.NewGuid();

            Services.QueueClient.Enqueue<IPawneeStage>(
                queue, Name,
                p => p.MergeData(chunkCount, mergeJobs),
                batch: mergeJobs,
                afterBatch: chunkJobs);

            Services.QueueClient.Enqueue<IPawneeStage>(
                queue, Name,
                p => p.MergeTracking(chunkCount),
                batch: mergeJobs,
                afterBatch: chunkJobs);
        }

        private async Task<int> PerformMerge<T>(
            string aspect, int batchCount, 
            IBlobMultiMap<T> target,
            IChangeListSaver changeList,
            Func<int, IBlobMultiMap<Command>> openCommandBatch)
        {
            var commands = Enumerable
                .Range(0, batchCount)
                .Select(n => openCommandBatch(n).IterateAll().LookAhead())
                .Merge((k1, k2) => StringComparer.OrdinalIgnoreCase
                                                 .Compare(k1.Key.ToString(), k2.Key.ToString()));

            var rate = new RateLogger(Services.Log, Name, aspect, 0, 1, TimeSpan.FromSeconds(5));

            string currentTargetGroup = null;

            while (await commands.GetNextBatch(10000))
            {            
                foreach (var (key, record) in commands.CurrentBatch)
                {
                    if (changeList != null && currentTargetGroup != key.Value)
                    {
                        currentTargetGroup = key.Value;

                        if (changeList.Add(key.Value))
                        {
                            await changeList.Flush();
                        }
                    }

                    if (record.Upsert == null)
                    {
                        await target.Delete(key);
                    }
                    else
                    {
                        await target.UpsertRaw(key, record.Upsert);
                    }

                    rate.Increment(key.Value);
                }
            }

            await Services.Log.ProgressLogged(new ProgressUpdate
            {
                Stage = Name,
                Aspect = aspect,
                Message = "Saving results..."
            });

            await target.SaveOverwrite();

            var chunkCount = changeList == null ? 0 : await changeList.Complete();

            await Services.Log.ProgressLogged(new ProgressUpdate
            {
                Stage = Name,
                Aspect = aspect,
                Message = $"Read {batchCount} chunks, produced {chunkCount}",
                Status = ProgressStatus.Succeeded
            });

            return chunkCount;
        }

        public async Task MergeData<T>(int batchCount, Guid mergeJobBatch)
        {
            var chunkCount = await PerformMerge("merging-data", batchCount,
                Services.Maps.OpenData<T>(Name, false),
                Services.ChangeLists.Save(Name),
                n => Services.Maps.OpenDataCommands(Name, n, false));

            await Services.Queues.InTransaction(queue => {
                Services.QueueClient.Enqueue<IPawneeOutput>(
                        queue, Name,
                        p => p.NotifyOutputs(chunkCount),
                        afterBatch: mergeJobBatch);
                });
        }

        public Task MergeTracking(int batchCount)
            => PerformMerge("merging-tracking", batchCount,
                Services.Maps.OpenTracking(Name, false), null,
                n => Services.Maps.OpenDataCommands(Name, n, false));        
    }
}
