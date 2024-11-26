using Nemo.IO;

namespace Nemo.Model;

public class ModelContext
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; }
    public string JobDirectory { get; set; }
    public SourceManager Sources{ get; set; }
    public Projection Projection { get; set; }
    public OutputSet OutputSet { get; set; }

    //public Job(string name, string jobDirectory, CSVSource records, Projection projection, OutputSet outputSet, IReadOnlyDictionary<string, CSVSource> tables)
    //{
    //    Name = name;
    //    JobDirectory = string.IsNullOrEmpty(jobDirectory)?Directory.GetCurrentDirectory():jobDirectory;
    //    Records = records;
    //    Projection = projection;
    //    OutputSet = outputSet;
    //    Tables = tables;
    //}
    public ModelContext(string name, string jobDirectory, Projection projection, OutputSet outputSet, SourceManager sourceManager)
    {
        Name = name;
        JobDirectory = string.IsNullOrEmpty(jobDirectory) ? Directory.GetCurrentDirectory() : jobDirectory;
        Projection = projection;
        OutputSet = outputSet;
        Sources = sourceManager;

        sourceManager.ExtractSources(Path.Combine(JobDirectory, "extracted"));
    }
}
