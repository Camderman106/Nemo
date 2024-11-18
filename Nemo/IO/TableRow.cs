namespace Nemo.IO;

public record TableRow
{
    public TableRow(int index, string[] values)
    {
        Index = index;
        Values = values;
    }

    public int Index { get; init; }
    public string[] Values { get; set; }
}
