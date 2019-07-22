namespace Pawnee.Core.BlobMaps
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public interface ITimings
    {
        IDisposable Track(string category);

        IDictionary<string, double> GetResults();
    }

    public class Timings : ITimings
    {
        private readonly Stopwatch _timer = new Stopwatch();

        public Timings() => _timer.Start();

        private Dictionary<string, double> _results = new Dictionary<string, double>();
        
        public class Timer : IDisposable
        {
            private readonly string _category;
            private readonly double _started;
            private readonly Timings _owner;

            public Timer(string category, double started, Timings owner)
            {
                _category = category;
                _started = started;
                _owner = owner;
            }

            public void Dispose() => _owner.Save(_category, _started);            
        }

        private void Save(string category, double started)
        {
            _results.TryGetValue(category, out var total);
            _results[category] = total + (_timer.Elapsed.Seconds - started);
        }

        public IDisposable Track(string category) => new Timer(category, _timer.Elapsed.Seconds, this);

        public IDictionary<string, double> GetResults()
        {
            var results = _results;
            _results = new Dictionary<string, double>();
            return results;
        }
    }
}
