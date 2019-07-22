namespace Pawnee.AzureBindings
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Core;
    using Core.BlobMaps;
    using Microsoft.Extensions.Logging;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class AzureBlobStorage : IBlobStorage
    {
        private readonly CloudBlobContainer _container;
        private readonly Lazy<Task> _createOnDemand;
        private readonly ILogger _logger;

        private const int RetryCount = 10;
        private static readonly TimeSpan RetryInterval = TimeSpan.FromSeconds(5);

        public AzureBlobStorage(
            IAzureStorageProvider azureStorage, 
            ILogger<AzureBlobStorage> logger, 
            string name)
        {
            _container = azureStorage.Account.CreateCloudBlobClient().GetContainerReference(name);
            _createOnDemand = new Lazy<Task>(() => _container.CreateIfNotExistsAsync());
            _logger = logger;
        }

        private async Task<CloudBlockBlob> Get(string key)
        {
            await _createOnDemand.Value;
            return _container.GetBlockBlobReference(key);
        }

        public Task Delete(string key) => Retry.Async(
            _logger, RetryCount, RetryInterval, $"Deleting blob {key} in {_container.Name}",
                async () => await (await Get(key)).DeleteIfExistsAsync());
        
        public Task<Stream> Read(string key) => Retry.Async(
            _logger, RetryCount, RetryInterval, $"Opening blob {key} in {_container.Name} for reading",
                async () => 
                {
                    var blob = await Get(key);
                    if (!await blob.ExistsAsync()) return null;
                    return await blob.OpenReadAsync(AccessCondition.GenerateEmptyCondition(), 
                                                    new BlobRequestOptions(),
                                                    new OperationContext());
                });
        
        private class MemoryStreamWithFlushedEvent : MemoryStream
        {
            public Func<Task> Flushed { get; set; }

            public override async Task FlushAsync(CancellationToken cancellationToken)
            {
                await base.FlushAsync(cancellationToken);
                await Flushed();
            }
        }

        public Task Store(string key, byte[] data) => Retry.Async(
                _logger, RetryCount, RetryInterval, $"Storing blob {key} in {_container.Name}",
                async () => await (await Get(key)).UploadFromByteArrayAsync(data, 0, data.Length));

        public Task<byte[]> Fetch(string key)
        {
            return Retry.Async(_logger, RetryCount, RetryInterval, $"Fetching blob {key}", async () =>
            {
                try
                {
                    var stream = new MemoryStream();
                    await (await Get(key)).DownloadToStreamAsync(stream);
                    return stream.ToArray();
                }
                catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
                {
                    return null;
                }
            });
        }

        public async Task Clear()
        {
            await _createOnDemand.Value;

            BlobContinuationToken cont = null;

            do
            {
                var page = await _container.ListBlobsSegmentedAsync(cont);

                var toDelete = page.Results.OfType<CloudBlockBlob>().ToList();
                const int batchSize = 50;

                for (var offset = 0; offset < toDelete.Count; offset += batchSize)
                {
                    await Task.WhenAll(
                        toDelete.Skip(offset)
                                .Take(batchSize)
                                .Select(b => _container.GetBlobReference(b.Name).DeleteAsync()));
                }

                cont = page.ContinuationToken;
            }
            while (cont != null);
        }
    }
}
