namespace Nemo.Model;

public class OutputSet
{
    public bool AggregatedOutput = true;
    public bool IndividualOutput = true;

    public List<string> Scalars { get; set; } = new() { "Scalars.All" };
    public List<string> Columns { get; set; } = new() { "Columns.All" };
}
