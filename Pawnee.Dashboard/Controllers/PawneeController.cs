using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using Pawnee.Core;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Pawnee.Core.Queue;

namespace Pawnee.Dashboard.Controllers
{
    using Core.BlobMaps;
    using Core.Pipelines;
    using MessagePack;

    [Route("api/[controller]")]
    public class PawneeController : Controller
    {
        private readonly IRedisConnection _redis;
        private readonly IPawneeQueueStorage _queue;
        private readonly IPawneeMapFactory _maps;

        public PawneeController(IRedisConnection redis,
                                IPawneeQueueStorage queue,
                                IPawneeMapFactory maps)
        {
            _redis = redis;
            _queue = queue;
            _maps = maps;
        }

        [HttpGet]
        [Route("pipeline")]
        public JToken Pipeline() => JToken.Parse(_redis.Database.StringGet("PawneePipeline"));

        [HttpGet]
        [Route("pipeline/{name}")]
        public async Task<ActionResult<Tabulation>> Stage(string name, int skip, int take, string filter)
        {
            var map = _maps.OpenData<byte[]>(name, true);

            return new ActionResult<Tabulation>(await Tabulate(map, skip, take, filter));
        }

        private int TabulateItem(byte[] data, int offset, List<string> row)
        {
            var type = MessagePackBinary.GetMessagePackType(data, offset);
            var result = string.Empty;
            int readSize;

            switch (type)
            {
                case MessagePackType.Array:
                    return TabulateArray(data, offset, row);

                case MessagePackType.Binary:
                    MessagePackBinary.ReadBytes(data, offset, out readSize);
                    break;

                case MessagePackType.Boolean:
                    result = $"{MessagePackBinary.ReadBoolean(data, offset, out readSize)}";
                    break;

                case MessagePackType.Float:
                    result = $"{MessagePackBinary.ReadDouble(data, offset, out readSize)}";
                    break;

                case MessagePackType.Integer:
                    result = $"{MessagePackBinary.ReadInt32(data, offset, out readSize)}";
                    break;

                case MessagePackType.Nil:
                    MessagePackBinary.ReadNil(data, offset, out readSize);
                    break;

                case MessagePackType.String:
                    result = MessagePackBinary.ReadString(data, offset, out readSize);
                    break;

                default:
                    readSize = MessagePackBinary.ReadNext(data, offset);
                    break;
            }

            row.Add(result);

            return offset + readSize;
        }

        private int TabulateArray(byte[] data, int offset, List<string> row)
        {
            var arrayLength = MessagePackBinary.ReadArrayHeader(data, offset, out var readSize);
            offset += readSize;

            for (var n = 0; n < arrayLength; n++)
            {
                offset = TabulateItem(data, offset, row);
            }

            return offset;
        }

        private async Task<Tabulation> Tabulate(IBlobMultiMap<byte[]> data, int skip, int take, string filter)
        {
            var rows = new List<List<string>>();

            using (var iter = data.IterateKeyRange(filter, null))
            {
                while (await iter.GetNextBatch(skip + take))
                {
                    foreach (var (key, record) in iter.CurrentBatch)
                    {
                        if (skip > 0)
                        {
                            skip--;
                        }
                        else if (take > 0)
                        {
                            take--;

                            var row = new List<string> {key.Value, key.Id};

                            TabulateItem(record, 0, row);

                            rows.Add(row);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }

            return new Tabulation
            {
                Columns = new[] { "Key", "Id" }, //.Concat(properties.Select(p => p.Name)),
                Rows = rows
            };
        }

        [HttpGet]
        [Route("queue")]
        public Task<IPawneeQueueState> Queue() => _queue.Read();

        [HttpGet]
        [Route("log")]
        public async Task<IEnumerable<ProgressUpdate>> Log(int skip, int take)
        {
            var list = await _redis.Database.ListRangeAsync("PawneeLogEntries", skip, skip + take - 1);
            return list.Select(str => JsonConvert.DeserializeObject<ProgressUpdate>(str));
        }
    }

    public class Tabulation
    {
        public IEnumerable<string> Columns { get; set; }

        public IEnumerable<IEnumerable<string>> Rows { get; set; }
    }
}
