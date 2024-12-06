using System.Diagnostics;

namespace Nemo.Model.Buffers;
internal class AggregateOutputBuffer
{
    internal Guid BufferId = Guid.NewGuid();
    internal string ModelClass { get; init; }
    internal string Group { get; set; }

    internal string GroupByKey => $"{ModelClass}|{Group}";
    internal AggregateOutputBuffer(string modelclass, string group)
    {
        ModelClass = modelclass;
        Group = group;
    }

    internal List<AggregateColumnBuffer?> ColumnBuffers { get; set; } = new(); //null if not output

    internal static AggregateOutputBuffer CreateEmptyDuplicate(AggregateOutputBuffer template)
    {
        AggregateOutputBuffer result = new AggregateOutputBuffer(template.ModelClass, template.Group);
        foreach (AggregateColumnBuffer? buffer in template.ColumnBuffers)
        {
            result.ColumnBuffers.Add(buffer?.CreateEmptyDuplicate());
        }
        return result;
    }
    public static AggregateOutputBuffer MergeBuffers(IEnumerable<AggregateOutputBuffer> others)
    {
        var first = others.First();
        var result = CreateEmptyDuplicate(first);
        foreach (AggregateOutputBuffer other in others)
        {
            if (other.ModelClass != result.ModelClass) throw new BufferException("Unable to merge buffers: Different ModelClass");
            if (other.Group != result.Group) throw new BufferException("Unable to merge buffers: Different Group");
            if (other.ColumnBuffers.Count != result.ColumnBuffers.Count) throw new BufferException("Unable to merge buffers: Column mismatch (how did this happen???");
            Debug.Assert(result.ColumnBuffers.Select(x => x?.ColumnName).SequenceEqual(other.ColumnBuffers.Select(x => x?.ColumnName)));
            for (int i = 0; i < result.ColumnBuffers.Count; i++)
            {
                AggregateColumnBuffer? a = result.ColumnBuffers[i];
                AggregateColumnBuffer? b = other.ColumnBuffers[i];
                Debug.Assert(a is not null && b is not null || a is null && b is null);
                if (a is null || b is null) continue;
                for (int j = 0; j < a.Values.Length; j++)
                {
                    a.Values[j].Sum += b.Values[j].Sum;
                    a.Values[j].Count += b.Values[j].Count;
                }
            }
        }
        return result;
    }
    internal static void Export(IEnumerable<AggregateOutputBuffer> buffers, ModelContext job)
    {
        var files = buffers.GroupBy(x => x.ModelClass);
        foreach (IGrouping<string, AggregateOutputBuffer> file in files)
        {
            string path = Path.Combine(job.JobDirectory, job.Name + "-" + file.Key + ".csv");

            var first = file.FirstOrDefault();
            if (first is null) { continue; }
            using var streamwriter = new StreamWriter(path);

            var headers = $"modelclass,group,time,{string.Join(',', first.ColumnBuffers.Select(x => x!.ColumnName))}";
            streamwriter.WriteLine(headers);

            foreach (var buffer in buffers)
            {
                for (int t = job.Projection.T_Start; t <= job.Projection.T_End; t++)
                {
                    int offset = t - job.Projection.T_Min;
                    string line = $"{buffer.ModelClass},{buffer.Group},{t},{string.Join(',', buffer.ColumnBuffers.Select(x => x.OutputValue(offset)))}";
                    streamwriter.WriteLine(line);
                }
            }
        }
    }
}
