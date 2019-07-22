using Microsoft.Extensions.DependencyInjection;
using System;
using Pawnee.Core;
using System.Threading.Tasks;
using Pawnee.Core.Queue;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using BlobMap;
using AzureBindings;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Azure = Microsoft.Azure.Management.Fluent.Azure;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;

namespace Pawnee.Watchdog
{
    class Program
    {
        static void Main(string[] args)
        {
            Main().Wait();
        }

        static async Task Main()
        {
            var config = new WatchdogConfiguration();

            var services = new ServiceCollection()
                    .AddBlobMap()
                    .AddAzureBindings(config.Storage)
                    .AddPawnee(config.Redis)
                    .AddLogging(l => l.AddConsole())
                    .BuildServiceProvider();

            var pawnee = services.GetRequiredService<IPawneeServices>();
            var log = services.GetRequiredService<ILogger<Program>>();

            var workers = await ListWorkers(config);
            var nextWorkerId = 1;
            const int maxWorkers = 50;

            var idle = new HashSet<string>();

            while (true)
            {
                var start = 0;
                var terminated = new List<string>();

                HashSet<string> active = null;

                await pawnee.Queues.InTransaction(queue =>
                {                    
                    var expiryAge = DateTime.UtcNow - TimeSpan.FromMinutes(2);

                    var unclaimed = queue.Items.Count(i => (i.Worker == null || i.Claimed < expiryAge) 
                                            && i.Stage[0] != '#');

                    start = Math.Min(unclaimed, maxWorkers - workers.Count);

                    active = queue.Items.Select(i => i.Worker)
                                  .Where(w => w != null)
                                  .ToHashSet();

                    if (start == 0)
                    {
                        foreach (var worker in workers.Where(w => !active.Contains(w) && idle.Contains(w)))
                        {
                            // second time we've seen it idle, request termination
                            log.LogInformation($"Requesting worker {worker} to quit");
                            queue.Enqueue($"#{worker}", "quit");
                        }
                    }

                    var terminatedJobs = queue.Items.ToList()
                                              .Where(i => i.Content == "terminated" && i.Stage[0] == '#');

                    foreach (var item in terminatedJobs)
                    {
                        var worker = item.Stage.Substring(1);
                        log.LogInformation($"Worker {worker} has terminated");
                        terminated.Add(worker);
                        queue.Release(item);
                    }
                });

                if (terminated.Count != 0)
                {
                    log.LogInformation($"Destroying {terminated.Count} container(s)");
                    await Task.WhenAll(terminated.Select(id => StopWorker(id, config)));
                    foreach (var w in terminated) workers.Remove(w);
                    log.LogInformation($"Finished destroying {terminated.Count} container(s)");
                }

                foreach (var w in workers)
                {
                    if (active.Contains(w))
                    {
                        if (idle.Remove(w))
                        {
                            log.LogInformation($"Worker {w} is no longer idle");
                        }
                    }
                    else
                    {
                        if (idle.Add(w))
                        {
                            log.LogInformation($"Worker {w} has become idle, might terminate soon");
                        }                        
                    }
                }

                if (start > 0)
                {
                    var newWorkers = Enumerable.Range(nextWorkerId, start)
                                               .Select(id => $"{id}")
                                               .ToList();
                    nextWorkerId += start;

                    log.LogInformation($"Starting {newWorkers.Count} container(s)");

                    await Task.WhenAll(newWorkers.Select(id => StartWorker(id, config)));
                    workers.AddRange(newWorkers);

                    log.LogInformation($"Started {newWorkers.Count} container(s)");
                }

                log.LogInformation($"Snoozing for 20 seconds...");
                await Task.Delay(TimeSpan.FromSeconds(20));
            }
        }

        private static DateTime _cacheCreated;
        private static Task<IAzure> _cachedAzure;

        private static Task<IAzure> Connect(WatchdogConfiguration config)
        {
            if (_cachedAzure != null && 
                !_cachedAzure.IsFaulted &&
                _cacheCreated.AddMinutes(10) > DateTime.UtcNow)
            {
                return _cachedAzure;
            }

            _cacheCreated = DateTime.UtcNow;
            _cachedAzure = ConnectDirectly(config);
            return _cachedAzure;
        }

        private static async Task<IAzure> ConnectDirectly(WatchdogConfiguration config)
        {            
            var tokens = new AzureServiceTokenProvider();
            var token = await tokens.GetAccessTokenAsync("https://management.core.windows.net/");
            var tokenCreds = new TokenCredentials(token);

            var creds = new AzureCredentials(tokenCreds, tokenCreds,
                            config.TenantId,
                            AzureEnvironment.AzureGlobalCloud);

            return Azure.Authenticate(creds).WithSubscription(config.Subscription);
        }

        private const string WorkerNamePrefix = "pawnee-worker-";

        static async Task<List<string>> ListWorkers(WatchdogConfiguration config)
        {
            var azure = await Connect(config);

            var list = await azure.ContainerGroups.ListByResourceGroupAsync(config.ResourceGroup);

            return list.Select(c => c.Name)
                       .Where(n => n.StartsWith(WorkerNamePrefix))
                       .Select(n => n.Substring(WorkerNamePrefix.Length))
                       .ToList();
        }

        static async Task StartWorker(string id, WatchdogConfiguration config)
        {
            var azure = await Connect(config);
            var resGroup = azure.ResourceGroups.GetByName(config.ResourceGroup);

            var name = $"pawnee-worker-{id}";

            await azure.ContainerGroups.Define(name)
                .WithRegion(resGroup.Region)
                .WithExistingResourceGroup(config.ResourceGroup)
                .WithLinux()
                .WithPublicImageRegistryOnly()
                .WithoutVolume()
                .DefineContainerInstance(name)
                    .WithImage(config.WorkerImage)
                    .WithoutPorts()
                    .WithCpuCoreCount(2)
                    .WithMemorySizeInGB(8)
                    .WithEnvironmentVariables(new Dictionary<string, string>
                    {
                        ["PAWNEE_AZSTORAGE"] = config.Storage,
                        ["PAWNEE_REDIS"] = config.Redis,
                        ["PAWNEE_WORKERID"] = id
                    })
                    .Attach()
                .CreateAsync();

            // Poll for the container group
            IContainerGroup containerGroup = null;
            while (containerGroup == null)
            {
                containerGroup = await azure.ContainerGroups.GetByResourceGroupAsync(config.ResourceGroup, name);
                await Task.Delay(1000);
            }

            // Poll until the container group is running
            while (containerGroup.State != "Running")
            {
                await containerGroup.RefreshAsync();
            }
        }

        static async Task StopWorker(string id, WatchdogConfiguration config)
        {
            var azure = await Connect(config);

            var name = $"pawnee-worker-{id}";

            await azure.ContainerGroups.DeleteByResourceGroupAsync(config.ResourceGroup, name);
        }
    }
}
