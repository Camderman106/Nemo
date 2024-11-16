using Nemo.IO;
using Nemo.Model;
using Nemo.Model.Components;
using nietras.SeparatedValues;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Nemo;

public class Engine<TModel> where TModel : ModelBase
{
    private int? ChunkSize = null;
    private bool MultiThreading = false;
    private int MaxThreads = 8;
    private string? GroupBy = null;
    private Func<Projection, OutputSet, TModel> ModelFactory;
    internal ConcurrentBag<AggregateOutputBuffer> AggregateBuffers { get; private set; }

    public Engine(Func<Projection, OutputSet, TModel> modelFactory)
    {
        ModelFactory = modelFactory;
        AggregateBuffers = new ConcurrentBag<AggregateOutputBuffer>();
    }
    public void Execute(Job job)
    {
        Stopwatch JobTimer = Stopwatch.StartNew();
        Directory.CreateDirectory(job.JobDirectory);
        var records = Table.From(job.Records).AsRecords();
        ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = MaxThreads };
        //Setting switch
        switch (GroupBy is not null, ChunkSize is not null, MultiThreading)
        {
            case (false, false, false):
                {
                    var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
                    ProcessBatch(instance, records);
                    break;
                }
            case (false, false, true):
                {
                    var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
                    ProcessBatch(instance, records);
                    break;
                }
            case (false, true, false):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.Chunk(ChunkSize!.Value).ToList();
                    foreach (var group in groups)
                    {
                        var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
                        ProcessBatch(instance, group);
                    }
                    break;
                }
            case (false, true, true):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.Chunk(ChunkSize!.Value).ToList();
                    Parallel.ForEach(groups, options, (group) =>
                    {
                        var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
                        ProcessBatch(instance, group);
                    });
                    break;
                }
            case (true, false, false):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.GroupBy(x => x[GroupBy!]).Select(x => x.ToList());
                    foreach (var group in groups)
                    {
                        var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
                        ProcessBatch(instance, group);
                    }
                    break;
                }
            case (true, false, true):
                {
                    IEnumerable<IEnumerable<TableRecord>> groups = records.GroupBy(x => x[GroupBy!]).Select(x => x.ToList());
                    Parallel.ForEach(groups, options, (group) =>
                    {
                        var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
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
                        var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
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
                        var instance = ModelFactory.Invoke(job.Projection, job.OutputSet);
                        ProcessBatch(instance, group);
                    });
                    break;
                }
        }
        
        //merge buffers again
        ConcurrentBag<AggregateOutputBuffer> bag = new();
        var buffergroups = AggregateBuffers.GroupBy(x => x.GroupByKey);
        foreach (var buffergroup in buffergroups)
        {
            bag.Add(AggregateOutputBuffer.MergeBuffers(buffergroup));
        }
        AggregateBuffers = bag;
        AggregateOutputBuffer.Export(bag, job);
        Console.WriteLine($"Job: {job.Name} completed in {(float)JobTimer.ElapsedMilliseconds/1000}s");
    }
    
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
        if (!instance.Initialised) instance.InitialiseBuffer(group);
        Stopwatch Odometer = Stopwatch.StartNew();
        Stopwatch BatchTimer = Stopwatch.StartNew();
        int BatchCount = 0;
        foreach (var record in records)
        {
            try
            {
                instance.RecordIndex++;
                instance.InjectModelData(record);
                instance.OnNextRecord();
                instance.Target();
                instance.OutputToBuffer();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Model point skipped {record.Index}");
                Console.WriteLine($"{ex.Message}");
            }
            finally
            {
                BatchCount++;
                if (BatchCount % 100 == 0)
                {
                    Console.WriteLine($"Odometer> {BatchCount}| ({Odometer.ElapsedMilliseconds}ms / 100 policies)");
                    Odometer.Reset();
                }
                instance.Reset();
            }            
        }
        if(instance.AggregateOutputBuffer is not null) 
            AggregateBuffers.Add(instance.AggregateOutputBuffer);
        Console.WriteLine($"Batch complete: Group '{group}' with {BatchCount} records took {(float)BatchTimer.ElapsedMilliseconds/1000}s");
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