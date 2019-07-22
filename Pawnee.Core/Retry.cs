namespace Pawnee.Core
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;


    public static class Retry
    {
        public static async Task<T> Async<T>(ILogger logger, int maxAttempts, TimeSpan between, string description, Func<Task<T>> operation)
        {
            var betweenMs = (int)between.TotalMilliseconds;

            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    if (attempt != 1)                    
                        logger?.LogInformation($"Starting '{description}' attempt {attempt}/{maxAttempts}");

                    var result = await operation();

                    if (attempt != 1)                    
                        logger?.LogInformation($"Succeeded '{description}' on attempt {attempt}/{maxAttempts}");

                    return result;
                }
                catch (Exception x) when (attempt != maxAttempts)
                {
                    var delay = TimeSpan.FromMilliseconds(betweenMs);
                    logger?.LogError(x, $"Failed '{description}' on attempt {attempt}/{maxAttempts}, will retry in {delay}");
                    await Task.Delay(delay);
                }

                betweenMs += Math.Min(60 * 1000, Math.Max(1, betweenMs / 5));
            }

            throw new InvalidOperationException(); // Actually unreachable
        }

        public static Task Async(ILogger logger, int maxAttempts, TimeSpan between, string description, Func<Task> operation)
        {
            return Async(logger, maxAttempts, between, description, async () => { await operation(); return 0; });
        }
    }
}
