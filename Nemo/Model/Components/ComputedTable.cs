using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Model.Components;
/// <summary>
/// Like a 2d column/mini model
/// </summary>
public class ComputedTable : IModelComponent
{
    public string Name { get; init; }
    private Func<int, int, double> EvaluationFunction;
    private double?[,] Values;

    public ComputedTable(string name, int colunms, int rows, Func<int, int, double> evaluationFunction)
    {
        Name = name;
        EvaluationFunction = evaluationFunction;
        Values = new double?[colunms, rows];
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public double At(int col, int row)
    {
        if (Values[col, row].HasValue)
        {
            return Values[col, row]!.Value;
        }
        else
        {
            double result = EvaluationFunction(col, row);
            Values[col, row] = result;
            return result;
        }
    }

    public bool IsCalculatedAt(int col, int row)
    {
        return Values[col, row].HasValue;
    }

    public double Peek(int col, int row)
    {
        if (Values[col, row].HasValue) return Values[col, row]!.Value;
        return 0;
    }

    public void Reset()
    {
        Array.Clear(Values, 0, Values.Length);
    }
}
