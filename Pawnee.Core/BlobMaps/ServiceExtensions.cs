namespace Pawnee.Core.BlobMaps
{
    using Microsoft.Extensions.DependencyInjection;

    public static class ServiceExtensions
    {
        public static IServiceCollection AddBlobMap(this IServiceCollection services)
            => services.AddSingleton<IBlobMapFactory, BlobMapFactory>()
                       .AddSingleton<ITimings, Timings>();
    }
}
