using Nemo.IO;
using Nemo.Model.Components;
using System.Reflection;

namespace Nemo.Model;
public abstract class ModelBase
{
    public string Name { get; init; } = nameof(ModelBase);
    public string Description { get; init; } = string.Empty;
    internal int RecordIndex = 0;
    internal bool Initialised = false;
    public static readonly double ZeroNoAverage = -1.23456789e300;
    public List<ModelBase> ChildModels { get; set; } = new();    
    private OutputSet OutputSet { get; init; }
    private Projection Projection { get; init; }
    private List<FieldInfo> ColumnFields { get; init; }
    private List<FieldInfo> ScalarFields { get; init; }
    private List<FieldInfo> ModelComponents { get; init; }
    internal AggregateOutputBuffer? AggregateOutputBuffer { get; set; }
    internal List<KeyValuePair<string, Action<string>>> DataScalarMap = new();
    public ModelBase(Projection projection, OutputSet outputSet)
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

        
    }
    protected void MapDataToScalar<T>(string dataField, Scalar<T> scalar, Func<string, T> converter) where T : notnull
    {
        DataScalarMap.Add(new KeyValuePair<string, Action<string>>(dataField, (dataValue) => scalar.SetValue(converter(dataValue))));
    }
    public virtual void OnNextRecord()
    {

    }

    public void InjectModelData(TableRecord record)
    {
        foreach (var field in DataScalarMap)
        {
            if (record.TryGetValue(field.Key, out var value))
            {
                field.Value.Invoke(value);
            }
            else
            {
                throw new InvalidDataException($"Field {field.Key} not found in data file");
            }
        }
    }

    internal void InitialiseBuffer(string group)
    {
        if (OutputSet.AggregatedOutput) //Set up aggregate buffer
        {
            AggregateOutputBuffer = new AggregateOutputBuffer(this.GetType().Name, group);
        }
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
            AggregateOutputBuffer.ColumnBuffers.Clear();
            for (int i = 0; i < ColumnFields.Count; i++)
            {
                FieldInfo field = ColumnFields[i];
                Column column = (Column)field.GetValue(this)!;
                if (column.isOutput)
                {
                    AggregateOutputBuffer.ColumnBuffers.Add(new AggregateColumnBuffer(column.Name, Projection.T_Start, Projection.T_End, column.Aggregation));
                }
                else
                {
                    AggregateOutputBuffer.ColumnBuffers.Add(null); //null if no output
                }
            }
        }
        Initialised = true;
    }
    public virtual void Target()
    {
        foreach (var columnField in ColumnFields)
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
                    var span = column.TrimForExport(Projection.T_Start, Projection.T_End - Projection.T_Start + 1);
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
            FieldInfo field = ModelComponents[i];
            IModelComponent component = (IModelComponent)field.GetValue(this)!;
            component.Reset();
        }
    }
}
