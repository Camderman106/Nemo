using Nemo.IO;

namespace Nemo.Model;

public class Projection
{
    public Projection(int min, int start, int end, int max, IReadOnlyDictionary<string, CSVSource> lookupData)
    {
        T_Min = min;
        T_Start = start;
        T_End = end;
        T_Max = max;
        Tables = lookupData;
    }

    public int T_Min { get; init; }
    public int T_Start { get; init; }
    public int T_End { get; init; }
    public int T_Max { get; init; }

    public IReadOnlyDictionary<string, CSVSource> Tables { get; set; }
}
