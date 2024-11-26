using Nemo.IO;
using Nemo.Model.Buffers;
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
    protected ModelContext Context { get; init; }
    private List<FieldInfo> ColumnFields { get; init; }
    private List<FieldInfo> ScalarFields { get; init; }
    private List<FieldInfo> ModelComponents { get; init; }
    internal AggregateOutputBuffer? AggregateOutputBuffer { get; set; }
    internal IndividualOutputBuffer? IndividualOutputBuffer { get; set; }
    internal List<KeyValuePair<string, Action<string>>> DataScalarMap = new();
    public ModelBase(ModelContext context)
    {
        Context = context;

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
        ChildModels.ForEach(x => x.InjectModelData(record));
    }

    internal void InitialiseBuffer(string group)
    {
        if (Context.OutputSet.AggregatedOutput) //Set up aggregate buffer
        {
            AggregateOutputBuffer = new AggregateOutputBuffer(this.GetType().Name, group);
            for (int i = 0; i < ColumnFields.Count; i++)
            {
                FieldInfo field = ColumnFields[i];
                Column column = (Column)field.GetValue(this)!;
                if (Context.OutputSet.Columns.Contains("Columns.All") || Context.OutputSet.Columns.Contains(column.Name))
                {
                    column.IsOutput = true;
                    AggregateOutputBuffer.ColumnBuffers.Add(new AggregateColumnBuffer(column.Name, Context.Projection.T_Start, Context.Projection.T_End, column.Aggregation));
                }
                else
                {
                    AggregateOutputBuffer.ColumnBuffers.Add(null); //null if no output
                }
            }
        }
        if (Context.OutputSet.IndividualOutput)
        {
            for (int i = 0; i < ScalarFields.Count; i++)
            {
                FieldInfo field = ScalarFields[i];
                ScalarBase scalar = (ScalarBase)field.GetValue(this)!;
                if (Context.OutputSet.Scalars.Contains("Scalars.All") || Context.OutputSet.Scalars.Contains(scalar.OutputName))
                {
                    scalar.IsOutput = true;
                }
            }
            var scalarsToOutput = ScalarFields.Select(x => (IScalarOutputToString)x.GetValue(this)!).Where(x => x.IsOutput).Select(x => x.OutputName).ToArray()!;
            IndividualOutputBuffer = new IndividualOutputBuffer(this.GetType().Name, scalarsToOutput);
        }
        Initialised = true;
        ChildModels.ForEach(x => x.InitialiseBuffer(group));
    }
    public virtual void Target()
    {
        foreach (var columnField in ColumnFields)
        {
            Column column = (Column)columnField.GetValue(this)!;
            for (var i = Context.Projection.T_Start; i <= Context.Projection.T_End; i++) 
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
                if (column.IsOutput)
                {
                    AggregateColumnBuffer columnBuffer = AggregateOutputBuffer.ColumnBuffers[i]!;
                    var span = column.TrimForExport(Context.Projection.T_Start, Context.Projection.T_End - Context.Projection.T_Start + 1);
                    for (int t = Context.Projection.T_Start; t <= Context.Projection.T_End; t++)
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
        if (IndividualOutputBuffer is not null)
        {
            IndividualOutputBuffer.OutputRecords.Add(
                new IndividualOutputRecord(RecordIndex,
                    string.Join(',',
                        ScalarFields.Select(x => (IScalarOutputToString)x.GetValue(this)!)
                        .Where(x => x.IsOutput)
                        .Select(x => x.OutputToString()))));

        }
        ChildModels.ForEach(x => x.OutputToBuffer());
    }
    internal void Reset()
    {
        for (int i = 0; i < ModelComponents.Count; i++)
        {
            FieldInfo field = ModelComponents[i];
            IModelComponent component = (IModelComponent)field.GetValue(this)!;
            component.Reset();
        }
        ChildModels.ForEach(x => x.Reset());
    }
}
