using Nemo.IO;
using Nemo.Model;
using nietras.SeparatedValues;
using System.Collections.Concurrent;

namespace Nemo;

public class Engine<TModel> where TModel : ModelBase
{
    private int? ChunkSize = null;
    private bool MultiThreading = false;
    private int MaxThreads = 8;
    private string? GroupBy = null;
    Func<Projection, OutputSet, TModel> ModelFactory;
    internal ConcurrentBag<AggregateOutputBuffer> AggregateBuffers { get; private set; } 

    public Engine(Func<Projection, OutputSet, TModel> modelFactory)
    {
        ModelFactory = modelFactory;
        AggregateBuffers = new ConcurrentBag<AggregateOutputBuffer>();
    }
    public void Execute(Job job)
    {
        CSVSource data = job.Records;
        var records = Table.From(data).IndexByRow().AsDataRecords();
        var instance = ModelFactory(job.Projection, job.OutputSet);
        foreach (var record in records)
        {
            InjectScalarData(instance, record);
        }
    }

    /// <summary>
    /// This method injects the scalars with data from each record without any unnecessary allocations making it fast as fuck
    /// Whilst its not ideal for the SepReader.Row to be a dependency here, its a ref struct. So we don't have much choice as it can't be passed to any other complex structure
    /// Given this limitation, responsibility within the model class didnt seem right, not did respondibility inside the CSVSource. Hence it is here in the engine
    /// </summary>
    /// <param name="instance"></param>
    /// <param name="record"></param>
    private static void InjectScalarData(TModel instance, SepReader.Row record) 
    {
        foreach (var dataField in instance.DataScalarMap)
        {
            dataField.Value.Invoke(record[dataField.Key].Span); //never even had to allocate it to a string!
        }
    }

    public Engine<TModel> UseMultiThreading(bool multiThreading)
    {
        this.MultiThreading = multiThreading;
        return this;
    }
    public Engine<TModel> UseChunkSize(int chunkSize)
    {
        this.ChunkSize = chunkSize;
        return this;
    }
    public Engine<TModel> MaximumThreads(int maxThreads)
    {
        this.MaxThreads = maxThreads;
        return this;
    }
    public Engine<TModel> GroupRecordsBy(string groupBy)
    {
        GroupBy = groupBy;
        return this;
    }

}
