using Microsoft.Extensions.DependencyInjection;

namespace BlobMap
{
    public static class ServiceExtensions
    {
        public static IServiceCollection AddBlobMap(this IServiceCollection services)
            => services.AddSingleton<IBlobMapFactory, BlobMapFactory>()
                       .AddSingleton<ITimings, Timings>();
    }
}
