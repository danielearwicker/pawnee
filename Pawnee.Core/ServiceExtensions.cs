using Microsoft.Extensions.DependencyInjection;
using Pawnee.Core.Queue;

namespace Pawnee.Core
{
    using Pipelines;

    public static class ServiceExtensions
    {
        public static IServiceCollection AddPawnee(this IServiceCollection services, string redis)
        {
            services
                .AddTransient<IChunkProcessorFactory, ChunkProcessorFactory>()
                .AddTransient<IPawneeMapFactory, PawneeMapFactory>()
                .AddTransient<IChangeListStorage, ChangeListStorage>()
                .AddTransient<IMethodCallSerializer, MethodCallSerializer>()
                .AddTransient<IPawneeConfig, PawneeConfig>()
                .AddTransient<IPawneeQueueClient, PawneeQueueClient>()
                .AddTransient<IPawneeScaleParameters, PawneeScaleParameters>()
                .AddTransient<IPawneeServices, PawneeServices>()
                .AddSingleton<IPawneeQueueStorage, PawneeQueueStorage>()
                .AddSingleton<IRedisConnection>(new RedisConnection(redis))
                .AddSingleton<IStatusEvents, StatusEvents>();

            services.AddSignalR()
                .AddJsonProtocol()
                .AddStackExchangeRedis(redis, options =>
                {
                    options.Configuration.ChannelPrefix = "Pawnee";
                });

            return services;
        }
    }
}
