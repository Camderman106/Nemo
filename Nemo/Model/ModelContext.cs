using Nemo.IO;
using System.Resources;

namespace Nemo.Model;

public class ModelContext
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; }
    public string JobDirectory { get; set; }
    public SourceManager Sources{ get; set; }
    public Projection Projection { get; set; }
    public OutputSet OutputSet { get; set; }
    public string? TraceResultsTable { get; private set; } = null;
    
    public ModelContext(string name, string jobDirectory, Projection projection, OutputSet outputSet, SourceManager sourceManager)
    {
        Name = name;
        JobDirectory = string.IsNullOrEmpty(jobDirectory) ? Directory.GetCurrentDirectory() : jobDirectory;
        Projection = projection;
        OutputSet = outputSet;
        Sources = sourceManager;

        sourceManager.ExtractSources(Path.Combine(JobDirectory, "extracted"));
        
        
    }
    public ModelContext SetTraceTable(string traceTable)
    {
        this.TraceResultsTable = "#~Reference~#";
        Sources.AddCSVSource("#~Reference~#", traceTable);
        Console.WriteLine("Ensure that the trace table contains only unaggregated groups. AKA one policy per group, or the tracing will fail");
        Sources.ExtractSources(Path.Combine(JobDirectory, "extracted")); //need to re-extract the table. Will only affect 1 table
        return this;
    }
}
