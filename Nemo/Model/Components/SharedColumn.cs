using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Nemo.Model.Components;

[DebuggerDisplay("{Name}")]
public class SharedColumn : IModelComponent
{
    public string Name { get; init; }
    internal ColumnValue[] Values;
    private Func<int, double> EvaluationFunction;
    private readonly int T_min;
    internal bool IsOutput { get; set; } = false;
    public SharedColumn(string outputName, ModelContext context, Func<int, double> evalFunction)
    {
        Name = outputName;
        T_min = context.Projection.T_Min;
        Values = new ColumnValue[context.Projection.T_Max - context.Projection.T_Min + 1];
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
                    Values[offset].Value = value;
                    Values[offset].State = ColumnValueState.Calculated;                    
                    return Values[offset].Value;
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
        //Array.Clear(Values, 0, Values.Length);
        //By default shared columns do not reset between policies. Hence 'Shared'
    }
    public void ResetShared()
    {
        Array.Clear(Values, 0, Values.Length);
    }
}
