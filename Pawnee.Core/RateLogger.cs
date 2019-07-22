using System;
using System.Diagnostics;

namespace Pawnee.Core
{
    public class RateLogger
    {
        private readonly Stopwatch _timer = new Stopwatch();
        private readonly IStatusEvents _logger;
        private readonly string _stage, _aspect;
        private readonly int _instance, _instanceCount;
        private readonly TimeSpan _interval;

        private int _loggedCount;

        public int Count { get; private set; }

        public RateLogger(IStatusEvents logger,
                          string stage, 
                          string aspect, 
                          int instance,
                          int instanceCount,
                          TimeSpan interval)
        {
            _timer.Start();
            _logger = logger;
            _stage = stage;
            _aspect = aspect;
            _instance = instance;
            _instanceCount = instanceCount;
            _interval = interval;
        }

        public bool Increment(string sample)
        {
            Count++;

            var elapsed = _timer.Elapsed.TotalSeconds;
            if (elapsed >= _interval.TotalSeconds)
            {
                var rate = (Count - _loggedCount) / elapsed;

                _logger.ProgressLogged(new ProgressUpdate
                {
                    Stage = _stage,
                    Aspect = _aspect,
                    Instance = _instance,
                    InstanceCount = _instanceCount,
                    Sample = sample,
                    Count = Count,
                    PerSecond = rate
                })
                .ContinueWith(x =>
                {
                    if (x.Exception != null) {  /* ignore? */ }
                });

                _timer.Restart();
                _loggedCount = Count;

                return true;
            }

            return false;
        }
    }
}
