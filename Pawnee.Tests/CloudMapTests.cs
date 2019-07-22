using FluentAssertions;
using Moq;
using Platform;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace BlobMap.Tests
{
    public class BlobMapTests
    {
        class MemoryStorage : IBlobStorage
        {
            public Dictionary<string, byte[]> Blobs = new Dictionary<string, byte[]>();

            public Task Clear()
            {
                Blobs.Clear();
                return Task.CompletedTask;
            }

            public Task Delete(string key)
            {
                Blobs.Remove(key);
                return Task.CompletedTask;
            }

            public Task<byte[]> Fetch(string key)
            {
                Blobs.TryGetValue(key, out var blob);
                return Task.FromResult(blob);
            }

            public async Task<Stream> Read(string key)
            {
                await Task.Delay(10);

                return Blobs.TryGetValue(key, out var blob)
                        ? new MemoryStream(blob)
                        : null;
            }

            public async Task Store(string key, byte[] data)
            {
                await Task.Delay(10);

                Blobs[key] = data;
            }
        }

        private readonly ITimings _timings = new Mock<ITimings>().Object;

        [Fact]
        public async Task IdentifiesMissingKey()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions());

            var result = await map.GetKey("missing");
            result.HasValue.Should().BeFalse();
        }

        [Fact]
        public async Task StoresAndRetrievesKeys()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions());

            await (await map.GetKey("missing")).UpdateValue("crazy value");

            (await map.GetKey("missing")).GetValue().Should().Be("crazy value");
        }

        [Fact]
        public async Task Persists()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions());

            await (await map.GetKey("missing")).UpdateValue("crazy value");

            await map.SaveOverwrite();

            map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions());

            (await map.GetKey("missing")).GetValue().Should().Be("crazy value");
        }

        [Fact]
        public async Task PersistsNew()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions());

            await (await map.GetKey("missing")).UpdateValue("crazy value");

            await map.SaveOverwrite();

            storage.Blobs.Keys.Should().Contain(new[] { "0", "1" });

            map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions());

            await (await map.GetKey("extra")).UpdateValue("sensible value");

            var saveResult = await map.SaveNew();

            storage.Blobs.Keys.Should().Contain(new[] { "0", "1", "2", "3" });

            await saveResult.DeleteGarbage();

            storage.Blobs.Keys.Should().Contain(new[] { "2", "3" });

            map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions { RootNodeId = saveResult.RootId });

            (await map.GetKey("missing")).GetValue().Should().Be("crazy value");
            (await map.GetKey("extra")).GetValue().Should().Be("sensible value");
        }

        [Fact]
        public async Task IteratesAllKeyValues()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions { MaxNodeKeys = 17 });

            const int count = 562;

            for (var n = 0; n < count; n++)
            {
                await (await map.GetKey($"{n:000}")).UpdateValue($"{n} value");
            }

            var i = map.IterateKeyValues(string.Empty);

            var results = new List<(string Key, string Value)>();

            while (await i.GetNextBatch(0))
            {
                results.AddRange(i.CurrentBatch);
            }

            for (var n = 0; n < count; n++)
            {
                results[n].Should().Be(($"{n:000}", $"{n} value"));
            }
        }

        [Fact]
        public async Task IteratesSubsetOfKeyValues()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions { MaxNodeKeys = 11 });

            const int count = 243;

            for (var n = 0; n < count; n++)
            {
                await (await map.GetKey($"{n:000}")).UpdateValue($"{n} value");
            }

            for (var s = 0; s < count - 1; s++)
            {
                var i = map.IterateKeyValues($"{s:000}");

                var results = new List<(string Key, string Value)>();

                while (await i.GetNextBatch(0))
                {
                    results.AddRange(i.CurrentBatch);
                }

                for (var n = 0; n < count - s; n++)
                {
                    results[n].Should().Be(($"{(n + s):000}", $"{n + s} value"));
                }
            }
        }

        [Fact]
        public async Task InteratesOneKeyPrefix()
        {
            var storage = new MemoryStorage();
            var map = new BlobMapTree<string>(storage, _timings, new BlobMapOptions { MaxNodeKeys = 11 });

            const int count = 50;

            for (var n = 0; n < count; n++)
            {
                await (await map.GetKey($"{n:000}")).UpdateValue($"{n} value");
            }

            var i = map.IterateKeyValues("01")
                       .TakeWhile(k => k.Key.StartsWith("01"));

            var results = new List<(string Key, string Value)>();

            while (await i.GetNextBatch(1))
            {
                results.AddRange(i.CurrentBatch);
            }
        }
    }
}
