namespace Pawnee.AzureBindings
{
    using Microsoft.WindowsAzure.Storage;

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
