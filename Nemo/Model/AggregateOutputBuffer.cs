using Nemo.Model.Components;
using System.Diagnostics;

namespace Nemo.Model;

internal class AggregateOutputBuffer
{
    internal Guid BufferId = Guid.NewGuid();
    internal string Group { get; set; }
    internal AggregateOutputBuffer(string group)
    {
        Group = group;
    }

    internal List<AggregateColumnBuffer?> ColumnBuffers { get; set; } = new(); //null if not output
}

internal class AggregateColumnBuffer
{
    internal AggregationMethod AggregationMethod { get; init; }
    internal AggregateColumnBufferValue[] Values { get; set; }
    internal AggregateColumnBuffer(int output_T_From, int output_T_To, AggregationMethod aggregation)
    {
        Values = new AggregateColumnBufferValue[output_T_To - output_T_From + 1];
        AggregationMethod = aggregation;
    }
    internal void Reset()
    {
        Array.Clear(Values, 0, Values.Length);
    }
}
[DebuggerDisplay("Sum({Sum})  Count({Count})")]
internal struct AggregateColumnBufferValue
{
    internal UInt32 Count;
    internal double Sum;
}
