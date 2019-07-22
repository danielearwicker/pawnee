using System;

namespace Pawnee.Watchdog
{
    public class WatchdogConfiguration
    {
        public string WorkerImage { get; }

        public string Storage { get; }

        public string Redis { get; }

        public string Subscription { get; }

        public string ResourceGroup { get; }

        public string TenantId { get; set; }

        public WatchdogConfiguration()
        {
            WorkerImage = Environment.GetEnvironmentVariable("PAWNEE_WORKERIMAGE");
            Storage = Environment.GetEnvironmentVariable("PAWNEE_AZSTORAGE");
            Redis = Environment.GetEnvironmentVariable("PAWNEE_REDIS");
            Subscription = Environment.GetEnvironmentVariable("PAWNEE_SUBSCRIPTION");
            ResourceGroup = Environment.GetEnvironmentVariable("PAWNEE_RESOURCEGROUP");
            TenantId = Environment.GetEnvironmentVariable("PAWNEE_TENANTID");
        }
    }
}
