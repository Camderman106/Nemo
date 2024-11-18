namespace Nemo.Model.Buffers;

internal class IndividualOutputBuffer
{
    internal Guid BufferId = Guid.NewGuid();
    internal string ModelClass { get; init; }
    internal string[] Headers { get; init; }
    internal List<IndividualOutputRecord> OutputRecords { get; init; } = new();
    public IndividualOutputBuffer(string modelClass, string[] headers)
    {
        ModelClass = modelClass;
        Headers = headers;
    }
    public static void Export(IEnumerable<IndividualOutputBuffer> buffers, Job job)
    {
        var files = buffers.GroupBy(x => x.ModelClass).ToList();
        foreach (var file in files)
        {
            string path = Path.Combine(job.JobDirectory, job.Name + "-" + file.Key + "-individual.csv");
            using var streamwriter = new StreamWriter(path);
            string headers = string.Join(',', file.First().Headers);
            streamwriter.WriteLine(headers);
            var recordsInOrder = file.SelectMany(x => x.OutputRecords).OrderBy(x => x.Index);
            foreach (var record in recordsInOrder)
            {
                streamwriter.WriteLine(record.Line);
            }
        }
    }
}
