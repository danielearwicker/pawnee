namespace Pawnee.Core.Pipelines
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Queue;

    public interface IPawneeConfig
    {
        PawneeStageBuilder<T> AddStage<T>(string name);

        IPawneeStage this[string name] { get; }

        Task Execute(string workId);
    }

    public class PawneeConfig : IPawneeConfig
    {
        private readonly IPawneeServices _services;
        private readonly IRedisConnection _redis;

        private readonly List<IPawneeStage> _stages = new List<IPawneeStage>();
        
        private void AddStage(IPawneeStage stage)
        {
            if (_stages.Any(s => s.Name == stage.Name))
            {
                throw new InvalidOperationException($"Duplicated stage name: {stage.Name}");
            }

            _stages.Add(stage);
        }

        public PawneeConfig(IPawneeServices services,
                             IRedisConnection redis)
        {
            _services = services;
            _redis = redis;
        }

        public PawneeStageBuilder<T> AddStage<T>(string name)
            => new PawneeStageBuilder<T>(_services, AddStage, name);

        public IPawneeStage this[string name] => _stages.Single(s => s.Name == name);

        private Task PublishPipeline()
        {
            var json = JsonConvert.SerializeObject(_stages.Select(s => s.Description));
            return _redis.Database.StringSetAsync("PawneePipeline", json);
        }

        public async Task Execute(string workerId)
        {
            await PublishPipeline();

            var stages = _stages.ToList();
            stages.Reverse();

            var terminated = false;

            while (!terminated)
            {
                var (stage, job) = await _services.Queues.InTransaction(queue =>
                {
                    var blah = queue.Items.Where(i => i.Stage[0] != '#').ToList();

                    var workerStage = $"#{workerId}";

                    var workerState = queue.Dequeue(workerStage, workerId);
                    if (workerState != null)
                    {
                        queue.Release(workerState);
                        queue.Enqueue(workerStage, "terminated");
                        terminated = true;
                        return (null, null);
                    }

                    foreach (var s in stages)
                    {
                        var j = s.ClaimJob(queue, workerId);
                        if (j != null) return (s, j);
                    }

                    return (null, null);
                });

                if (terminated) return;
                
                if (job == null)
                {   
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
                else
                {
                    await stage.Execute(job);
                }
            }
        }
    }

    public class PawneeStageBuilder<TOut>
    {
        private readonly IPawneeServices _services;
        private readonly Action<IPawneeStage> _add;
        private readonly string _name;

        public PawneeStageBuilder(IPawneeServices services, Action<IPawneeStage> add, string name)
        {
            _services = services;
            _add = add;
            _name = name;
        }

        public PawneeProjection<TIn, TOut> From<TIn>(params IPawneeStage<TIn>[] inputs)
        {
            var stage = new PawneeProjection<TIn, TOut>(_services, _name, inputs);
            _add(stage);
            return stage;
        }

        public PawneeSource<TOut> Parse(Func<Stream, Func<string, TOut, Task>, Task> parser)
        {
            var stage = new PawneeSource<TOut>(_services, _name, parser);
            _add(stage);
            return stage;
        }
    }
}
