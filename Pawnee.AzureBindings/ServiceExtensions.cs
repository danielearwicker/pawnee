namespace Pawnee.AzureBindings
{
    using Core;
    using Core.BlobMaps;
    using Microsoft.Extensions.DependencyInjection;

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
