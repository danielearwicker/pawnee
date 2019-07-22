namespace FileSystemBindings
{
    public interface IFilePathProvider
    {
        string Path { get; }
    }

    public class FilePathProvider : IFilePathProvider
    {
        public string Path { get; }

        public FilePathProvider(string path) => Path = path;
    }
}
