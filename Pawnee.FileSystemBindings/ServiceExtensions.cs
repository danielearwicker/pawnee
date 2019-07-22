using BlobMap;
using Microsoft.Extensions.DependencyInjection;
using Platform;

namespace FileSystemBindings
{
    public static class ServiceExtensions
    {
        public static IServiceCollection AddFileSystemBindings(
            this IServiceCollection services,
            string rootPath)
                => services
                    .AddSingleton<IFilePathProvider>(new FilePathProvider(rootPath))                    
                    .AddFactory<IBlobStorage, FileStorage, string>();
    }
}
