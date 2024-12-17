namespace Nemo.Model;

public class OutputSet
{
    public bool AggregatedOutput=false;
    public bool IndividualOutput=false;

    public List<string> Scalars { get; set; } = [];
    public List<string> Columns { get; set; } = [];
    public OutputSet()
    {

    }
    public OutputSet WithScalars(List<string> scalars)
    {
        this.IndividualOutput = true;
        this.Scalars = scalars;
        return this;
    }
    public OutputSet WithColumns(List<string> columns)
    {
        this.AggregatedOutput = true;
        this.Columns = columns; 
        return this;
    }
    public static OutputSet Default()
    {
        return new OutputSet()
        {
            AggregatedOutput = true,
            IndividualOutput = true,
            Scalars = new() { "Scalars.All" },
            Columns = new() { "Columns.All" },
        };
    }
}
