namespace Pawnee.Core
{
    using System;
    using Microsoft.Extensions.DependencyInjection;

    public static class FactoryExtensions
    {
        public static IServiceCollection AddFactory<TInterface, TImplementation>(this IServiceCollection services)
            where TImplementation : class, TInterface
            where TInterface : class => services.AddSingleton<Func<TInterface>>(sp => ()
                                            => ActivatorUtilities.CreateInstance<TImplementation>(sp));

        public static IServiceCollection AddFactory<TInterface, TImplementation, TArg1>(this IServiceCollection services)
            where TImplementation : class, TInterface
            where TInterface : class => services.AddSingleton<Func<TArg1, TInterface>>(sp => arg1
                                            => ActivatorUtilities.CreateInstance<TImplementation>(sp, arg1));

        public static IServiceCollection AddFactory<TInterface, TImplementation, TArg1, TArg2>(this IServiceCollection services)
            where TImplementation : class, TInterface
            where TInterface : class => services.AddSingleton<Func<TArg1, TArg2, TInterface>>(sp => (arg1, arg2)
                                            => ActivatorUtilities.CreateInstance<TImplementation>(sp, arg1, arg2));

        public static IServiceCollection AddFactory<TInterface, TImplementation, TArg1, TArg2, TArg3>(this IServiceCollection services)
            where TImplementation : class, TInterface
            where TInterface : class => services.AddSingleton<Func<TArg1, TArg2, TArg3, TInterface>>(sp => (arg1, arg2, arg3)
                                            => ActivatorUtilities.CreateInstance<TImplementation>(sp, arg1, arg2, arg3));
    }
}
