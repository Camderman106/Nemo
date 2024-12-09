using Nemo.IO;
using Nemo.Model;
using Nemo.Model.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime;
using System.Runtime.CompilerServices;
using Table = Nemo.IO.CSV.Table;

namespace Nemo;

public class Engine<TModel> where TModel : ModelBase
{
    private int? ChunkSize = null;
    private bool MultiThreading = false;
    private int MaxThreads = 8;//Environment.ProcessorCount;
    private string? GroupBy = null;
    private Func<ModelContext, TModel> ModelFactory;
    internal ConcurrentBag<AggregateOutputBuffer> AggregateBuffers { get; private set; }
    internal ConcurrentBag<IndividualOutputBuffer> IndividualBuffers { get; private set; }
    public Engine(Func<ModelContext, TModel> modelFactory)
    {
        ModelFactory = modelFactory;
        AggregateBuffers = new ConcurrentBag<AggregateOutputBuffer>();
        IndividualBuffers = new ConcurrentBag<IndividualOutputBuffer>();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]

    public void Execute(ModelContext context)
    {
        Stopwatch JobTimer = Stopwatch.StartNew();
        Directory.CreateDirectory(context.JobDirectory);
        if(context.Sources.Data is null) throw new ArgumentNullException(nameof(context.Sources.Data));
        var records = Table.From(context.Sources.Data).AsRecords();
        ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = MaxThreads };
        //Setting switch

        switch (GroupBy is not null, ChunkSize is not null, MultiThreading)
        {
            case (false, false, false):
                {
                    var instance = ModelFactory.Invoke(context);
                    ProcessBatch(instance, records);
                    break;
                }
            case (false, false, true):
                {
                    var instance = ModelFactory.Invoke(context);
                    ProcessBatch(instance, records);
                    break;
                }
            case (false, true, false):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.Chunk(ChunkSize!.Value).ToList();
                    foreach (var group in groups)
                    {
                        var instance = ModelFactory.Invoke(context);
                        ProcessBatch(instance, group);
                    }
                    break;
                }
            case (false, true, true):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.Chunk(ChunkSize!.Value).ToList();
                    Parallel.ForEach(groups, options, (group) =>
                    {
                        var instance = ModelFactory.Invoke(context);
                        ProcessBatch(instance, group);
                    });
                    break;
                }
            case (true, false, false):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.GroupBy(x => x[GroupBy!]).Select(x => x.ToList());
                    foreach (var group in groups)
                    {
                        var instance = ModelFactory.Invoke(context);
                        ProcessBatch(instance, group);
                    }
                    break;
                }
            case (true, false, true):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.GroupBy(x => x[GroupBy!]).Select(x => x.ToList());
                    Parallel.ForEach(groups, options, (group) =>
                    {
                        var instance = ModelFactory.Invoke(context);
                        ProcessBatch(instance, group);
                    });
                    break;
                }
            case (true, true, false):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.GroupBy(x => x[GroupBy!]).Select(x => x.ToList());
                    groups = groups.SelectMany(x => x.Chunk(ChunkSize!.Value)).ToList();
                    foreach (var group in groups)
                    {
                        var instance = ModelFactory.Invoke(context);
                        ProcessBatch(instance, group);
                    }
                    break;
                }
            case (true, true, true):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.GroupBy(x => x[GroupBy!]).Select(x => x.ToList());
                    groups = groups.SelectMany(x => x.Chunk(ChunkSize!.Value)).ToList();
                    Parallel.ForEach(groups, options, (group) =>
                    {
                        var instance = ModelFactory.Invoke(context);
                        ProcessBatch(instance, group);
                    });
                    break;
                }
        }

        //merge aggregate buffers again
        {
            ConcurrentBag<AggregateOutputBuffer> bag = new();
            var buffergroups = AggregateBuffers.GroupBy(x => x.GroupByKey);
            foreach (var buffergroup in buffergroups)
            {
                bag.Add(AggregateOutputBuffer.MergeBuffers(buffergroup));
            }
            AggregateBuffers = bag;
        }

        if (context.OutputSet.AggregatedOutput) AggregateOutputBuffer.Export(AggregateBuffers, context);
        if (context.OutputSet.IndividualOutput) IndividualOutputBuffer.Export(IndividualBuffers, context);
        Console.WriteLine($"Job: {context.Name} completed in {(float)JobTimer.ElapsedMilliseconds / 1000}s");
    }
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]

    public void ProcessBatch(TModel instance, IEnumerable<TableRecord> records)
    {
        string group = "";
        if (GroupBy is not null)
        {
            if (records.First().ContainsKey(GroupBy))
            {
                group = records.First()[GroupBy];                
            }
            else
            {
                throw new EngineException("Groupby column not found");
            }
        }
        Console.WriteLine($"Batch start Group: '{group}', Thread: {Thread.CurrentThread.ManagedThreadId}");
        if (!instance.Initialised) instance.InitialiseBuffer(group);
        Stopwatch Odometer = Stopwatch.StartNew();
        Stopwatch BatchTimer = Stopwatch.StartNew();
        int BatchCount = 0;
        foreach (var record in records)
        {
            #if DEBUG            
                instance.RecordIndex = record.Index - 1;
                instance.InjectModelData(record);
                instance.OnNextRecord();
                instance.Target();
                instance.OutputToBuffer();            
            
                BatchCount++;
                if (BatchCount % 100 == 0)
                {
                    Console.WriteLine($"Odometer> {BatchCount}| ({Odometer.ElapsedMilliseconds}ms / 100 policies)");
                    Odometer.Restart();
                }
                instance.Reset();

            #else

            try
            {
                instance.RecordIndex = record.Index -1;
                instance.InjectModelData(record);
                instance.OnNextRecord();
                instance.Target();
                instance.OutputToBuffer();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Model point skipped: {instance.RecordIndex}");
                Console.WriteLine($"{ex.Message}");
            }
            finally
            {
                BatchCount++;
                if (BatchCount % 100 == 0)
                {
                    Console.WriteLine($"Odometer> {BatchCount}| ({Odometer.ElapsedMilliseconds}ms / 100 policies)");
                    Odometer.Restart();
                }
                instance.Reset();
            }
            #endif

        }
        if (instance.AggregateOutputBuffer is not null)
            AggregateBuffers.Add(instance.AggregateOutputBuffer);
        if (instance.IndividualOutputBuffer is not null)
            IndividualBuffers.Add(instance.IndividualOutputBuffer);
        Console.WriteLine($"Batch complete: Group '{group}' with {BatchCount} records took {(float)BatchTimer.ElapsedMilliseconds / 1000}s");
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
internal class EngineException : Exception
{
    public EngineException()
    {
    }

    public EngineException(string? message) : base(message)
    {
    }

    public EngineException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}