using nietras.SeparatedValues;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
namespace Nemo.IO;

public class Table
{
    public CSVSource CSVSource { get; private set; } = null!;
    private Encoding Encoding => CSVSource.Encoding;
    private string FilePath => CSVSource.FilePath;
    private string LineTerminator => CSVSource.LineTerminator;
    public static Table From(CSVSource cSVSource)
    {
        return new Table() { CSVSource = cSVSource };
    }

    public class ColumnIndexed : IDisposable
    {
        internal CSVSource CSVSource { get; private set; }
        internal IReadOnlyDictionary<StringArrayKey, long> OffsetMap { get; init; }
        internal IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; }
        public bool HasHeaders { get; init; }
        private StreamReader StreamReader { get; init; }
        private SepReader SepReader { get; set; }
        private int ColumnCount { get; init; }
        public ColumnIndexed(CSVSource source, IReadOnlyDictionary<StringArrayKey, long> offsetMap, bool hasHeaders)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            StreamReader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096));
            HasHeaders = hasHeaders;
            StreamReader.BaseStream.Seek(0, SeekOrigin.Begin);
            SepReader = Sep.Auto.Reader(o => (o with { HasHeader = true })).From(StreamReader);
            ColumnIndexMap = Enumerable.Range(0, SepReader.Header.ColNames.Count).Select(x => new KeyValuePair<string, int>(SepReader.Header.ColNames[x], x)).ToDictionary();
            ColumnCount = SepReader.Header.ColNames.Count;
        }

        public string LookupString(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false, CharsMinimumLength = 1024 * 4 })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}");
            }
        }
        public string LookupString(string[] lookupColumnValues, string returnColumn, Func<string> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false, CharsMinimumLength = 1024 * 4 })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                return Fallback();
            }
        }
        public double LookupDouble(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false, CharsMinimumLength = 1024 * 4 })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}");
            }
        }
        public double LookupDouble(string[] lookupColumnValues, string returnColumn, Func<double> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false, CharsMinimumLength = 1024 * 4 })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                return Fallback();
            }
        }
        public int LookupInt(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false, CharsMinimumLength = 1024 * 4 })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}");
            }
        }
        public int LookupInt(string[] lookupColumnValues, string returnColumn, Func<int> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false, CharsMinimumLength = 1024 * 4 })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                return Fallback();
            }
        }

        public void Dispose()
        {
            StreamReader?.Dispose();
        }

    }

    public ColumnIndexed IndexByColumns(string[] indexByColumns) //headers implicitly required
    {
        var offsetMap = CreateIndexByColumns(indexByColumns);
        return new ColumnIndexed(CSVSource, offsetMap, true);
    }
    public ColumnIndexed IndexByColumns(int[] indexByColumns, bool hasHeaders = true)
    {
        var offsetMap = CreateIndexByColumns(indexByColumns);
        return new ColumnIndexed(CSVSource, offsetMap, hasHeaders);
    }

    internal IReadOnlyDictionary<StringArrayKey, long> CreateIndexByColumns(string[] columnHeadersToIndex)
    {
        bool DetectedDuplicateKeys = false;
        Dictionary<StringArrayKey, long> RowPositions = new();
        using (var fs = new FileStream(FilePath, FileMode.Open, FileAccess.Read))
        {
            using (var reader = Sep.Auto.Reader(o => o with { HasHeader = true }).From(fs))
            {
                long offset = Encoding.GetPreamble().Length;
                offset += Encoding.GetByteCount(reader.Header.ToString()) + Encoding.GetByteCount(LineTerminator);
                while (reader.MoveNext())
                {
                    SepReader.Row row = reader.Current;
                    var indexCols = reader.Header.IndicesOf(columnHeadersToIndex);
                    var indexValues = reader.Current[indexCols];
                    var key = new StringArrayKey(indexValues.ToStringsArray());
                    if (DetectedDuplicateKeys == false && RowPositions.ContainsKey(key))
                    {
                        DetectedDuplicateKeys |= true;
                        Console.WriteLine("Warning: Duplicate keys detected while indexing file. Earlier values will be overwritten");
                    }
                    RowPositions[key] = offset;
                    offset += Encoding.GetByteCount(row.Span) + Encoding.GetByteCount(LineTerminator);
                }
            }
        }
        return RowPositions;
    }
    internal IReadOnlyDictionary<StringArrayKey, long> CreateIndexByColumns(int[] columnIndexesToIndex, bool hasHeader = true)
    {
        bool DetectedDuplicateKeys = false;
        Dictionary<StringArrayKey, long> RowPositions = new();
        using (var fs = new FileStream(FilePath, FileMode.Open, FileAccess.Read))
        {
            using (var reader = Sep.Auto.Reader(o => o with { HasHeader = hasHeader }).From(fs))
            {
                long offset = Encoding.GetPreamble().Length;
                if (hasHeader)
                {
                    //Count the header
                    offset += Encoding.GetByteCount(reader.Header.ToString()) + Encoding.GetByteCount(LineTerminator);
                }
                while (reader.MoveNext())
                {
                    SepReader.Row row = reader.Current;
                    var indexValues = reader.Current[columnIndexesToIndex];
                    var key = new StringArrayKey(indexValues.ToStringsArray());
                    if (DetectedDuplicateKeys == false && RowPositions.ContainsKey(key))
                    {
                        DetectedDuplicateKeys |= true;
                        Console.WriteLine("Warning: Duplicate keys detected while indexing file. Earlier values will be overwritten");
                    }
                    RowPositions[key] = offset;
                    offset += Encoding.GetByteCount(row.Span) + Encoding.GetByteCount(LineTerminator);
                }

            }
        }
        return RowPositions;
    }
    internal IReadOnlyDictionary<int, long> CreateIndex(bool hasHeader = true)
    {
        Dictionary<int, long> RowPositions = new Dictionary<int, long>();
        using (var fs = new FileStream(FilePath, FileMode.Open, FileAccess.Read))
        {
            using (var reader = Sep.Auto.Reader(o => o with { HasHeader = hasHeader }).From(fs))
            {
                long offset = Encoding.GetPreamble().Length;
                if (hasHeader)
                {
                    //Count the header
                    offset += Encoding.GetByteCount(reader.Header.ToString()) + Encoding.GetByteCount(LineTerminator);
                }
                while (reader.MoveNext())
                {
                    SepReader.Row row = reader.Current;
                    RowPositions[row.RowIndex] = offset; //1 is always the first record
                    offset += Encoding.GetByteCount(row.Span) + Encoding.GetByteCount(LineTerminator);
                }
            }
        }
        return RowPositions;
    }

    public IEnumerable<TableRecord> AsRecords()
    {
        using var reader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
        using var sep = Sep.Auto.Reader().From(reader);
        var header = sep.Header.ColNames;
        IReadOnlyDictionary<string, int> headerIndex = header.Zip(Enumerable.Range(0, header.Count)).ToDictionary(x => x.First, x => x.Second);
        foreach (var row in sep)
        {
            yield return new TableRecord(row.LineNumberFrom, row[sep.Header.ColNames].ParseToArray<string>(), headerIndex);
        }
    }
}

internal class LookupException : Exception
{
    public LookupException()
    {
    }

    public LookupException(string? message) : base(message)
    {
    }

    public LookupException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}

public class StringArrayKey : IEquatable<StringArrayKey>
{
    internal string[] Strings { get; init; }

    public StringArrayKey(string[] strings)
    {
        Strings = strings ?? throw new ArgumentNullException(nameof(strings));
    }
    public StringArrayKey(ReadOnlySpan<string> strings)
    {
        Strings = strings.ToArray() ?? throw new ArgumentNullException(nameof(strings));
    }

    public bool Equals(StringArrayKey? other)
    {
        if (other is null)
            return false;

        // Check if the arrays have the same length and elements
        return Strings.SequenceEqual(other.Strings);
    }

    public override bool Equals(object? obj)
    {
        if (obj is StringArrayKey otherWrapper)
            return Equals(otherWrapper);
        return false;
    }

    public override int GetHashCode()
    {
        // Generate a hash code based on the elements in the array
        return Strings.Aggregate(0, (hash, str) => hash ^ (str?.GetHashCode() ?? 0));
    }

    public override string ToString()
    {
        return $"[{string.Join(", ", Strings)}]";
    }
}

public record TableRow
{
    public TableRow(int index, string[] values)
    {
        Index = index;
        Values = values;
    }

    public int Index { get; init; }
    public string[] Values { get; set; }
}
public record TableRecord : TableRow, IReadOnlyDictionary<string, string>
{
    private IReadOnlyDictionary<string, int> HeaderIndex;
    public TableRecord(int index, string[] values, IReadOnlyDictionary<string, int> headerIndex) : base(index, values)
    {
        HeaderIndex = headerIndex;
    }

    public string this[string key] => Values[HeaderIndex[key]];

    public IEnumerable<string> Keys => HeaderIndex.Keys;

    public int Count => HeaderIndex.Count;

    IEnumerable<string> IReadOnlyDictionary<string, string>.Values => HeaderIndex.Values.Select(x => Values[x]);

    public bool ContainsKey(string key)
    {
        return HeaderIndex.ContainsKey(key);
    }

    public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
    {
        return HeaderIndex.Keys.Select(x => new KeyValuePair<string, string>(x, this[x])).GetEnumerator();
    }

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
    {
        if (HeaderIndex.TryGetValue(key, out int index))
        {
            value = Values[index];
            return true;
        }
        value = null;
        return false;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return ((IEnumerable)HeaderIndex).GetEnumerator();
    }
}
