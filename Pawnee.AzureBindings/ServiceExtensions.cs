using BlobMap;
using Microsoft.Extensions.DependencyInjection;
using Platform;

namespace AzureBindings
{
    public static class ServiceExtensions
    {
        public static IServiceCollection AddAzureBindings(
            this IServiceCollection services,
            string connectionString)
                => services
                    .AddSingleton<IAzureStorageProvider>(new AzureStorageProvider(connectionString))
                    .AddFactory<IBlobStorage, AzureBlobStorage, string>();
    }
}
