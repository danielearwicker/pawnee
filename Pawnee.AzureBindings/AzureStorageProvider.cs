using Microsoft.WindowsAzure.Storage;

namespace AzureBindings
{
    public interface IAzureStorageProvider
    {
        CloudStorageAccount Account { get; }
    }

    public class AzureStorageProvider : IAzureStorageProvider
    {
        public CloudStorageAccount Account { get; }

        public AzureStorageProvider(string connectionString)
            => Account = CloudStorageAccount.Parse(connectionString);
    }
}
