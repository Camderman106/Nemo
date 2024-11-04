using Nemo.Model.Components;
using nietras.SeparatedValues;
using System.Reflection;
using System.Transactions;

namespace Nemo.Model;
public abstract class ModelBase
{
    internal List<KeyValuePair<string, Action<ReadOnlySpan<char>>>> DataScalarMap = new();
    public static readonly double ZeroNoAverage = -1.23456789e300;
    public List<ModelBase> ChildModels { get; set; } = new();
    public string Name { get; init; } = nameof(ModelBase);
    public string Description { get; init; } = string.Empty;
    public Projection Projection { get; init; }
    private List<FieldInfo> ColumnFields { get; init; }
    private List<FieldInfo> ScalarFields { get; init; }
    private List<FieldInfo> ModelComponents { get; init; }
    private OutputSet OutputSet { get; init; }
    internal AggregateOutputBuffer? AggregateOutputBuffer { get; set; }
    internal int RecordIndex = 0;
    public ModelBase(Projection projection, OutputSet outputSet, string group)
    {
        Projection = projection;
        OutputSet = outputSet;

        // Fetching all Column fields
        ColumnFields = this.GetType()
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .Where(f => f.FieldType == typeof(Column)).ToList();

        // Fetching all Scalar fields
        ScalarFields = this.GetType()
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .Where(f => f.FieldType.IsGenericType && f.FieldType.GetGenericTypeDefinition() == typeof(Scalar<>)).ToList();

        ModelComponents = this.GetType()
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .Where(f => typeof(IModelComponent).IsAssignableFrom(f.FieldType)).ToList();

        if (OutputSet.AggregatedOutput) //Set up aggregate buffer
        {
            AggregateOutputBuffer = new AggregateOutputBuffer(group);
        }
    }
    
    protected void MapDataToScalar<T>(string dataField, Scalar<T> scalar, Func<ReadOnlySpan<char>, T> converter) where T: notnull
    {
        DataScalarMap.Add(new KeyValuePair<string, Action<ReadOnlySpan<char>>>(dataField, (dataValue) => scalar.SetValue(converter(dataValue))));
    } 
    internal void Initialise()
    {
        for (int i = 0; i < ColumnFields.Count; i++)
        {
            FieldInfo field = ColumnFields[i];
            Column column = (Column)field.GetValue(this)!;
            column.ColumnIndex = i;
            if (OutputSet.Columns.Contains("Columns.All") || OutputSet.Columns.Contains(column.Name))
            {
                column.isOutput = true;
            }
        }
        if (AggregateOutputBuffer is not null) //Set up aggregate buffer
        {
            for (int i = 0; i < ColumnFields.Count; i++)
            {
                FieldInfo field = ColumnFields[i];
                Column column = (Column)field.GetValue(this)!;
                if (column.isOutput)
                {
                    AggregateOutputBuffer.ColumnBuffers.Add(new AggregateColumnBuffer(Projection.T_Start, Projection.T_End, column.Aggregation));
                }
                else
                {
                    AggregateOutputBuffer.ColumnBuffers.Add(null); //null if no output
                }
            }
        }
    } 
    public virtual void Target()
    {
        foreach(var columnField in ColumnFields)
        {
            Column column = (Column)columnField.GetValue(this)!;
            for (var i = Projection.T_Start; i <= Projection.T_End; i++)
            {
                column.At(i);
            }
        }        
    }
    
    internal void OutputToBuffer()
    {
        if (AggregateOutputBuffer is not null)
        {
            for (int i = 0; i < ColumnFields.Count; i++)
            {
                FieldInfo field = ColumnFields[i];
                Column column = (Column)field.GetValue(this)!;
                AggregateColumnBuffer? columnBuffer = AggregateOutputBuffer.ColumnBuffers[i];
                if (columnBuffer is not null)
                {
                    var span = column.TrimForExport(Projection.T_Start, Projection.T_End-Projection.T_Start+1);
                    for (int t = Projection.T_Start; t <= Projection.T_End; t++)
                    {
                        if (span[t].State == ColumnValueState.Calculated)
                        {
                            columnBuffer.Values[t].Sum += span[t].Value;
                            columnBuffer.Values[t].Count++;
                        }
                        //uncalculated and no average are both treated as 'do nothing'. return 0 is calculated
                    }
                }
            }
        }
    }
    internal void Reset()
    {
        for (int i = 0; i < ModelComponents.Count; i++)
        {
            FieldInfo field = ColumnFields[i];
            IModelComponent component = (IModelComponent)field.GetValue(this)!;
            component.Reset();
        }
    }
}
