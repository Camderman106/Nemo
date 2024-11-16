using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Nemo.Model.Components;
[DebuggerDisplay("{Name}")]
public class Column : IModelComponent
{
    public string Name { get; init; }
    public int ColumnIndex { get; internal set; }
    internal ColumnValue[] Values;
    private Func<int, double> EvaluationFunction;
    private readonly int T_min;
    public readonly AggregationMethod Aggregation;
    internal bool IsOutput { get; set; } = false;
    public Column(string outputName, int t_min, int t_max, AggregationMethod aggregation, Func<int, double> evalFunction)
    {
        Name = outputName;
        T_min = t_min;
        Values = new ColumnValue[t_max - t_min + 1];
        Aggregation = aggregation;
        EvaluationFunction = evalFunction;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public double At(int t)
    {
        int offset = t - T_min;
        switch (Values[offset].State)
        {
            case ColumnValueState.Uncalculated:
                {
                    double value = EvaluationFunction(t);
                    if (value == ModelBase.ZeroNoAverage)
                    {
                        Values[offset].Value = 0;
                        Values[offset].State = ColumnValueState.ZeroNoAverage;
                    }
                    else
                    {
                        Values[offset].Value = value;
                        Values[offset].State = ColumnValueState.Calculated;
                    }
                    return Values[offset].Value;
                }
            case ColumnValueState.ZeroNoAverage:
                {
                    return 0;
                }
            case ColumnValueState.Calculated:
                {
                    return Values[offset].Value;
                }
            default:
                throw new InvalidOperationException();
        }
    }
    internal ReadOnlySpan<ColumnValue> TrimForExport(int start_t, int length)
    {
        return new ReadOnlySpan<ColumnValue>(Values, start_t - T_min, length);
    }
    public bool IsCalculatedAt(int t)
    {
        return Values[t - T_min].State > 0;
    }
    internal double View(int t)
    {
        return Values[t - T_min].Value;
    }
    public void Reset()
    {
        Array.Clear(Values, 0, Values.Length);
    }
}
public enum AggregationMethod
{
    Sum,
    Average
}
[DebuggerDisplay("{State}    {Value}")]
internal struct ColumnValue
{
    internal ColumnValueState State { get; set; }
    internal double Value { get; set; }
}
internal enum ColumnValueState : byte
{
    Uncalculated = 0b00000000,
    ZeroNoAverage = 0b00000001,
    Calculated = 0b00000010
}
