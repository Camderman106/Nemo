using Nemo.IO;

namespace Nemo.Model;

public class Job
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; }
    public string JobDirectory { get; set; }
    public CSVSource Records { get; set; }
    public IDictionary<string, CSVSource> OtherLookupData { get; set; }
    public Projection Projection { get; set; }
    public OutputSet OutputSet { get; set; }
    public Job(string name, string jobDirectory, CSVSource records, IDictionary<string, CSVSource> otherLookupData, Projection projection, OutputSet outputSet)
    {
        Name = name;
        JobDirectory = jobDirectory;
        Records = records;
        OtherLookupData = otherLookupData;
        Projection = projection;
        OutputSet = outputSet;
    }
}
