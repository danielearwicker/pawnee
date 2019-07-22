namespace Pawnee.FileSystemBindings
{
    using System.IO;
    using System.Threading.Tasks;
    using Core.BlobMaps;

    public class FileStorage : IBlobStorage
    {
        private readonly string _containerPath;

        public FileStorage(IFilePathProvider filePathProvider, string name)
        {            
            _containerPath = Path.Combine(filePathProvider.Path, name);
            Directory.CreateDirectory(_containerPath);
        }

        private string MakePath(string key) => Path.Combine(_containerPath, key);

        public Task Delete(string key) => Task.Run(() => File.Delete(MakePath(key)));

        public Task<byte[]> Fetch(string key) => Task.Run(() => 
            !File.Exists(MakePath(key)) ? null : File.ReadAllBytes(MakePath(key)));
        
        public Task<Stream> Read(string key) => Task.Run(() =>
            !File.Exists(MakePath(key)) ? (Stream)null : new FileStream(MakePath(key), FileMode.Open, FileAccess.Read));
        
        public Task Store(string key, byte[] data) => Task.Run(() =>
            File.WriteAllBytes(MakePath(key), data));

        public Task Clear()
        {
            foreach (var file in Directory.EnumerateFiles(_containerPath))
            {
                File.Delete(file);
            }
            return Task.CompletedTask;
        }
    }
}
