using MessagePack;
using System;

namespace Pawnee.Core.Queue
{
    public interface IPawneeQueueItem
    {
        Guid Id { get; }

        Guid Batch { get; }

        Guid? AfterBatch { get; }

        string Stage { get; }

        string From { get; }

        string Content { get; }

        string Error { get; }

        int FailureCount { get; }

        DateTime Enqueued { get; }

        DateTime? Started { get; }

        DateTime? Claimed { get; }

        string Worker { get; }
    }

    [MessagePackObject]
    public class PawneeQueueItem : IPawneeQueueItem
    {
        [Key(0)]
        public Guid Id { get; set; }

        [Key(1)]
        public Guid Batch { get; set; }

        [Key(2)]
        public Guid? AfterBatch { get; set; }

        [Key(3)]
        public string Stage { get; set; }

        [Key(4)]
        public string From { get; set; }

        [Key(5)]
        public string Content { get; set; }

        [Key(6)]
        public string Error { get; set; }

        [Key(7)]
        public int FailureCount { get; set; }

        [Key(8)]
        public DateTime Enqueued { get; set; }

        [Key(9)]
        public DateTime? Started { get; set; }

        [Key(10)]
        public DateTime? Claimed { get; set; }

        [Key(11)]
        public string Worker { get; set; }
    }
}
