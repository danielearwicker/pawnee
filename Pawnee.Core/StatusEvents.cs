using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Pawnee.Core.Queue;
using StackExchange.Redis;

namespace Pawnee.Core
{
    using BlobMaps;

    public enum ProgressStatus
    {
        Busy,
        Succeeded,
        Failed
    }

    public class ProgressUpdate
    {
        public string Stage { get; set; }
        public string Aspect { get; set; }
        public int Instance { get; set; }
        public int InstanceCount { get; set; }
        public string Sample { get; set; }
        public int? Count { get; set; }
        public double? PerSecond { get; set; }
        public string Message { get; set; }
        public ProgressStatus Status { get; set; } = ProgressStatus.Busy;
        public DateTime TimeStamp { get; set; }
        public long SequenceNumber { get; set; }
        public long VirtualBytes { get; set; }
        public long WorkingSet { get; set; }
    }

    public interface IStatusEvents
    {
        Task QueueUpdated(int version, IReadOnlyList<IPawneeQueueItem> items);

        Task ProgressLogged(ProgressUpdate update);
    }

    public class StatusEvents : IStatusEvents
    {
        private readonly IHubContext<StatusEventsHub, IStatusEvents> _hub;
        private readonly IRedisConnection _redis;
        private readonly ILogger<StatusEvents> _logger;
        private readonly ITimings _timings;

        public StatusEvents(IHubContext<StatusEventsHub, IStatusEvents> hub,
                            IRedisConnection redis,
                            ILogger<StatusEvents> logger,
                            ITimings timings)
        {
            _hub = hub;
            _redis = redis;
            _logger = logger;
            _timings = timings;
        }

        private const string LogVersion = nameof(Pawnee) + nameof(LogVersion);
        private const string LogEntries = nameof(Pawnee) + nameof(LogEntries);

        public async Task ProgressLogged(ProgressUpdate update)
        {
            string updateJson = null;

            update.TimeStamp = DateTime.UtcNow;

            var proc = Process.GetCurrentProcess();
            update.VirtualBytes = proc.VirtualMemorySize64;
            update.WorkingSet = proc.WorkingSet64;

            _logger.LogInformation(JsonConvert.SerializeObject(_timings.GetResults(), Formatting.Indented));

            await Retry.Async(_logger, 100, TimeSpan.FromSeconds(0.5), nameof(ProgressLogged), async () =>
            {
                long? seqNo = null;

                {
                    var result = await _redis.Database.StringGetAsync(LogVersion);
                    if (result.HasValue && long.TryParse(result.ToString(), out var s))
                    {
                        seqNo = s;
                    }
                }

                var transaction = _redis.Database.CreateTransaction();

                if (seqNo == null)
                {
                    transaction.AddCondition(Condition.KeyNotExists(LogVersion));
                }
                else
                {
                    transaction.AddCondition(Condition.StringEqual(LogVersion, $"{seqNo}"));
                }

                seqNo = (seqNo ?? -1) + 1;

                update.SequenceNumber = seqNo.Value;

                updateJson = JsonConvert.SerializeObject(update);

                _ = transaction.StringSetAsync(LogVersion, $"{seqNo}");
                _ = transaction.ListLeftPushAsync(LogEntries, updateJson);
                _ = transaction.ListTrimAsync(LogEntries, 0, 100000);

                if (!await transaction.ExecuteAsync()) throw new InvalidOperationException("Commit failed");
            });

            _logger.LogInformation(updateJson);

            await _hub.Clients.All.ProgressLogged(update);
        }
    
        public Task QueueUpdated(int version, IReadOnlyList<IPawneeQueueItem> items)
                => _hub.Clients.All.QueueUpdated(version, items);
    }

    public class StatusEventsHub : Hub<IStatusEvents> { }
}
