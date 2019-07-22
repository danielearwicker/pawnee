using Moq;
using Pawnee.Core;
using System.Threading.Tasks;
using Xunit;

namespace Pawnee.Tests
{
    using Core.Batched;
    using Core.BlobMaps;
    using Core.Pipelines;

    public class ChunkProcessorTests
    {
        public class TestRecord
        {
            public string Data { get; set; }
        }

        private readonly Mock<IPawneeMapFactory> _maps = new Mock<IPawneeMapFactory>();

        private readonly Mock<IBlobMultiMap<Command>> _trackingCommands = new Mock<IBlobMultiMap<Command>>();

        private readonly Mock<IBlobMultiMap<Command>> _dataCommands = new Mock<IBlobMultiMap<Command>>();

        private readonly Mock<IBlobMultiMap<MultiMapKey>> _tracking = new Mock<IBlobMultiMap<MultiMapKey>>();

        private readonly Mock<IPawneeScaleParameters> _scale = new Mock<IPawneeScaleParameters>();

        private readonly ChunkProcessorOptions _options = new ChunkProcessorOptions
        {
            ChunkId = 15,
            Stage = "boing"
        };

        private readonly IChunkProcessor<TestRecord> _subject;

        public ChunkProcessorTests()
        {
            _maps.Setup(m => m.OpenTrackingCommands(_options.Stage, _options.ChunkId, true))
                 .Returns(_trackingCommands.Object);

            _maps.Setup(m => m.OpenDataCommands(_options.Stage, _options.ChunkId, true))
                 .Returns(_dataCommands.Object);

            _maps.Setup(m => m.OpenTracking(_options.Stage, true))
                 .Returns(_tracking.Object);

            _subject = new ChunkProcessor<TestRecord>(_maps.Object, _scale.Object,
                new Mock<IStatusEvents>().Object, _options);
        }

        [Fact]
        public async Task ClearsAndSaves()
        {
            _dataCommands.Verify(x => x.Clear(), Times.Once);
            _trackingCommands.Verify(x => x.Clear(), Times.Once);

            await _subject.Save();

            _dataCommands.Verify(x => x.SaveOverwrite(), Times.Once);
            _trackingCommands.Verify(x => x.SaveOverwrite(), Times.Once);
        }

        [Fact]
        public async Task DeletesGroup()
        {
            _scale.SetupGet(s => s.TrackingReadBatchSize).Returns(5);

            var flapEnum = new Mock<IBatchedEnumerator<(MultiMapKey key, MultiMapKey value)>>();
            flapEnum.SetupSequence(x => x.GetNextBatch(5))
                .ReturnsAsync(true)
                .ReturnsAsync(false);

            flapEnum.SetupGet(x => x.CurrentBatch).Returns(new[]
            {
                (new MultiMapKey("flap-1"), new MultiMapKey("horse-2")),
                (new MultiMapKey("flap-3"), new MultiMapKey("mouse-4"))
            });

            _tracking.Setup(x => x.IterateKey("flap")).Returns(flapEnum.Object);

            await _subject.DeleteGroup("flap");

            _trackingCommands.Verify(x => x.Upsert(
                It.IsAny<MultiMapKey>(),
                It.IsAny<Command>()), Times.Exactly(2));

            _dataCommands.Verify(x => x.Upsert(
                It.IsAny<MultiMapKey>(),
                It.IsAny<Command>()), Times.Exactly(2));

            _trackingCommands.Verify(x => x.Upsert(
                It.Is<MultiMapKey>(m => m.Value == "flap" && m.Id == "1"),
                It.Is<Command>(c => c.Upsert == null)), Times.Once);

            _dataCommands.Verify(x => x.Upsert(
                It.Is<MultiMapKey>(m => m.Value == "horse" && m.Id == "2"),
                It.Is<Command>(c => c.Upsert == null)), Times.Once);

            _trackingCommands.Verify(x => x.Upsert(
                It.Is<MultiMapKey>(m => m.Value == "flap" && m.Id == "3"),
                It.Is<Command>(c => c.Upsert == null)), Times.Once);

            _dataCommands.Verify(x => x.Upsert(
                It.Is<MultiMapKey>(m => m.Value == "mouse" && m.Id == "4"),
                It.Is<Command>(c => c.Upsert == null)), Times.Once);
        }

        [Fact]
        public async Task AddsToGroup()
        {
            var key = new MultiMapKey("123", "456");

            _dataCommands.Setup(x => x.Add("humphrey", It.IsAny<Command>()))
                         .ReturnsAsync(key);

            await _subject.AddGroup("humphrey", "chimpden", new TestRecord { Data = "blah" });

            _trackingCommands.Verify(x => x.Add("chimpden", It.Is<Command>(c => c.Upsert.ToString() == "123-456")));
        }
    }
}
