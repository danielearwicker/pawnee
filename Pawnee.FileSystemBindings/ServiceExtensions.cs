namespace Pawnee.FileSystemBindings
{
    using Core;
    using Core.BlobMaps;
    using Microsoft.Extensions.DependencyInjection;

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
