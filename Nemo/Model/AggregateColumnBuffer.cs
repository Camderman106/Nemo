using Nemo.Model.Components;

namespace Nemo.Model;

internal class AggregateColumnBuffer
{
    internal string ColumnName;
    internal AggregationMethod AggregationMethod { get; init; }
    internal AggregateColumnBufferValue[] Values { get; set; }
    internal AggregateColumnBuffer(string columnName, int output_T_From, int output_T_To, AggregationMethod aggregation)
    {
        ColumnName = columnName;
        Values = new AggregateColumnBufferValue[output_T_To - output_T_From + 1];
        AggregationMethod = aggregation;
    }
    internal AggregateColumnBuffer CreateEmptyDuplicate()
    {
        return new AggregateColumnBuffer(ColumnName, 0 , Values.Length - 1, AggregationMethod);        
    }
    internal double OutputValue(int offset)
    {
        if(AggregationMethod == AggregationMethod.Sum) return Values[offset].Sum;
        return Values[offset].Sum / Values[offset].Count;
    }
}
