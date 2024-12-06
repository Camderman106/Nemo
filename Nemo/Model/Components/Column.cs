using System.Diagnostics;
using System.Runtime.CompilerServices;
using Nemo.IO.CSV;

namespace Nemo.Model.Components;
[DebuggerDisplay("{Name}")]
public class Column : IModelComponent
{
    private ModelBase Parent;
    public string Name { get; init; }
    internal ColumnValue[] Values;
    private Func<int, double> EvaluationFunction;
    private readonly int T_min;
    public readonly AggregationMethod Aggregation;
    internal bool IsOutput = false;

    public delegate double AtMethod(int t);
    public AtMethod At;
    private Table.ColumnIndexed? TraceTable = null!;

    public Column(ModelBase parent, string outputName, ModelContext context, AggregationMethod aggregation, Func<int, double> evalFunction)
    {
        Parent = parent;
        Name = outputName;
        T_min = context.Projection.T_Min;
        Values = new ColumnValue[context.Projection.T_Max - context.Projection.T_Min + 1];
        Aggregation = aggregation;
        EvaluationFunction = evalFunction;
        if(context.TraceResultsTable is null)
        {
            At = StdAt;
            TraceTable = null!;
        }
        else
        {
            At = TraceAt;
            TraceTable = Parent.TraceTable;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public double StdAt(int t)
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
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public double TraceAt(int t)
    {
        int offset = t - T_min;
        switch (Values[offset].State)
        {
            case ColumnValueState.Uncalculated:
                {
                    double value = EvaluationFunction(t);
                    double? trace = TraceTable!.LookupTrace([Parent.Name, Parent.Group, t.ToString()], Name);
                    if (trace.HasValue)
                    {
                        if(!(trace.Value == value))
                        {
                            Debugger.Break(); //Step over
                            EvaluationFunction(t); //Step into
                        }
                    }
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
    public double Peek(int t)
    {
        return Values[t - T_min].Value;
    }
    void IModelComponent.Reset()
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
