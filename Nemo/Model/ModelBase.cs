using DocumentFormat.OpenXml.Bibliography;
using Nemo.IO;
using Nemo.IO.CSV;
using Nemo.Model.Buffers;
using Nemo.Model.Components;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Nemo.Model;
public abstract class ModelBase
{
    public string Name { get; init; }
    public string Group { get; private set; } = String.Empty;
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

    internal Table.ColumnIndexed? TraceTable;

    public ModelBase(ModelContext context)
    {
        Name = this.GetType().Name;
        Context = context;
        if(Context.TraceResultsTable is not null) TraceTable = Table.From(Context.Sources.Tables[Context.TraceResultsTable]).IndexByColumns(["modelclass", "group", "time"]);

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

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal void InitialiseBuffer(string group)
    {
        if (Context.OutputSet.AggregatedOutput) //Set up aggregate buffer
        {
            AggregateOutputBuffer = new AggregateOutputBuffer(Name, group);
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
        Group = group;
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
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]

    internal void OutputToBuffer()
    {
        if (AggregateOutputBuffer is not null)
        {
            int t_start = Context.Projection.T_Start; //force local to avoid duplicate reference following
            int t_end = Context.Projection.T_End;
            for (int i = 0; i < ColumnFields.Count; i++)
            {
                FieldInfo field = ColumnFields[i];
                Column column = (Column)field.GetValue(this)!;
                if (column.IsOutput)
                {
                    AggregateColumnBuffer columnBuffer = AggregateOutputBuffer.ColumnBuffers[i]!;
                    var span = column.TrimForExport(Context.Projection.T_Start, Context.Projection.T_End - Context.Projection.T_Start + 1);
                    for (int t = t_start; t <= t_end; t++)
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
