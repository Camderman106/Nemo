using nietras.SeparatedValues;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
namespace Nemo.IO;

public class Table
{
    public CSVSource CSVSource { get; private set; } = null!;
    public static Table From(CSVSource cSVSource)
    {
        return new Table() { CSVSource = cSVSource };
    }
    public class Sequential : IDisposable
    {
        internal CSVSource CSVSource { get; private set; }
        private FileStream FileStream { get; init; }
        private SepReader SepReader { get; init; }
        internal IReadOnlyDictionary<int, long> OffsetMap { get; init; }
        public bool HasHeaders { get; init; }
        public Sequential(CSVSource source, IReadOnlyDictionary<int, long> offsetMap, bool hasHeaders)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            HasHeaders = hasHeaders;
            FileStream = new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            SepReader = Sep.Auto.Reader().From(FileStream);
        }
        
        public IEnumerable<IReadOnlyDictionary<string, string>> AsRecords()
        {
            using var reader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
            using var sep = Sep.Auto.Reader().From(FileStream);
            var header = sep.Header.ColNames;
            foreach (var row in sep)
            {
                KeyValuePair<string, string>[] kvp = new KeyValuePair<string, string>[header.Count];
                for (int i = 0; i < header.Count; i++)
                {
                    kvp[i] = new KeyValuePair<string, string>(header[i], row[i].ToString());
                }
                yield return new Dictionary<string, string>(kvp).ToFrozenDictionary();
            }
        }
        public void Dispose()
        {
            SepReader?.Dispose();
            FileStream?.Dispose();
        }

    }
    public class ColumnIndexed : IDisposable
    {
        internal CSVSource CSVSource { get; private set; }
        internal IReadOnlyDictionary<StringArrayKey, long> OffsetMap { get; init; }
        internal IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; }
        public bool HasHeaders { get; init; }
        private StreamReader StreamReader { get; init; }
        private SepReader SepReader { get; set; }
        public string LastDebugMessage { get; private set; } = string.Empty;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string LookupString(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                throw new LookupException(LastDebugMessage);
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string LookupString(string[] lookupColumnValues, string returnColumn, Func<string> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return Fallback();
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double LookupDouble(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                throw new LookupException(LastDebugMessage);
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double LookupDouble(string[] lookupColumnValues, string returnColumn, Func<double> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return Fallback();
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int LookupInt(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                throw new LookupException(LastDebugMessage);
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int LookupInt(string[] lookupColumnValues, string returnColumn, Func<int> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                StreamReader.BaseStream.Seek(offset, SeekOrigin.Begin);
                SepReader = Sep.Auto.Reader(o => (o with { HasHeader = false })).From(StreamReader);
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return Fallback();
            }
        }

        public void Dispose()
        {
            StreamReader?.Dispose();
        }

    }

    public Sequential IndexSequential(bool hasHeaders = true)
    {
        var offsetMap = CreateIndex(hasHeaders);
        return new Sequential(CSVSource, offsetMap, hasHeaders);
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
    private Encoding Encoding => CSVSource.Encoding;
    private string FilePath => CSVSource.FilePath;
    private string LineTerminator => CSVSource.LineTerminator;
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

