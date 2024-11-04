using System;
using System.Collections.Frozen;
using System.Collections.ObjectModel;
using System.Text;
using Nemo.Model.Components;
using nietras.SeparatedValues;
namespace Nemo.IO;

public class Table
{
    public CSVSource CSVSource { get; private set; } = null!;
    public static Table From(CSVSource cSVSource)
    {
        return new Table() { CSVSource = cSVSource };
    }
    public class Sequential: IDisposable
    {
        internal CSVSource CSVSource { get; private set; }
        private FileStream FileStream { get; init; }
        private SepReader SepReader { get; init; }
        public Sequential(CSVSource source, Dictionary<int, long> offsetMap, bool hasHeaders)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            HasHeaders = hasHeaders;
        }
        internal Dictionary<int, long> OffsetMap { get; init; }
        public bool HasHeaders { get; init; }
        internal IEnumerable<SepReader.Row> AsDataRecords()
        {
            using var reader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
            using var sep = Sep.Auto.Reader().From(FileStream);
            var header = sep.Header.ColNames;
            foreach (var row in sep)
            {
                yield return row;
            }
        }
        public IEnumerable<FrozenDictionary<string, string>> AsRecords()
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
    public class HeaderIndexed :  IDisposable
    {
        internal CSVSource CSVSource { get; private set; }
        public string[] IndexColumns { get; init; }
        internal Dictionary<StringArrayKey, long> OffsetMap { get; init; }
        public bool HasHeaders = true;
        private FileStream FileStream { get; init; }
        private SepReader SepReader { get; init; }
        private Func<ReadOnlySpan<Char>> DefaultFallback { get; init; } 
        public string LastDebugMessage { get; private set; } = string.Empty;

        public HeaderIndexed(CSVSource source, string[] indexColumns, Dictionary<StringArrayKey, long> offsetMap)
        {
            CSVSource = source;
            IndexColumns = indexColumns;
            OffsetMap = offsetMap;
            FileStream = new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            SepReader = Sep.Auto.Reader().From(FileStream);
            DefaultFallback = () => throw new LookupException(LastDebugMessage);
        }

        public ReadOnlySpan<char> Lookup(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset) )
            {
                FileStream.Seek(offset, SeekOrigin.Begin);
                SepReader.MoveNext();
                return SepReader.Current[returnColumn].Span;
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return DefaultFallback();
            }
        }
        public ReadOnlySpan<char> Lookup(string[] lookupColumnValues, string returnColumn, Func<ReadOnlySpan<Char>> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                FileStream.Seek(offset, SeekOrigin.Begin);
                SepReader.MoveNext();
                return SepReader.Current[returnColumn].Span;
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return Fallback();
            }
        }
        public IEnumerable<FrozenDictionary<string, string>> AsRecords()
        {
            using var reader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
            using var sep = Sep.Auto.Reader().From(FileStream);
            var header = sep.Header.ColNames;
            foreach (var row in sep)
            {
                KeyValuePair<string, string>[] kvp= new KeyValuePair<string, string>[header.Count];
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
    public class ColumnIndexed :  IDisposable
    {
        internal CSVSource CSVSource { get; private set; }
        public int[] IndexColumns { get; init; }
        internal Dictionary<StringArrayKey, long> OffsetMap { get; init; }
        public bool HasHeaders { get; init; }
        private FileStream FileStream { get; init; }
        private SepReader SepReader { get; init; }
        private Func<ReadOnlySpan<Char>> DefaultFallback { get; init; }
        public string LastDebugMessage { get; private set; } = string.Empty;

        public ColumnIndexed(CSVSource source, int[] indexColumns, Dictionary<StringArrayKey, long> offsetMap, bool hasHeaders)
        {
            CSVSource = source;
            IndexColumns = indexColumns;
            OffsetMap = offsetMap;
            FileStream = new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            SepReader = Sep.Auto.Reader(o => o with { HasHeader = hasHeaders}).From(FileStream);
            DefaultFallback = () => throw new LookupException(LastDebugMessage);
            HasHeaders = hasHeaders;
        }
        public ReadOnlySpan<char> Lookup(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                FileStream.Seek(offset, SeekOrigin.Begin);
                SepReader.MoveNext();
                return SepReader.Current[returnColumn].Span;
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return DefaultFallback();
            }
        }
        public ReadOnlySpan<char> Lookup(string[] lookupColumnValues, string returnColumn, Func<ReadOnlySpan<Char>> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                FileStream.Seek(offset, SeekOrigin.Begin);
                SepReader.MoveNext();
                return SepReader.Current[returnColumn].Span;
            }
            else
            {
                LastDebugMessage = $"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.FilePath)} with key: {key}";
                return Fallback();
            }
        }
        public IEnumerable<FrozenDictionary<string, string>> AsRecords()
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

    public Sequential IndexByRow(bool hasHeaders = true)
    {
        var offsetMap = Index(hasHeaders);
        return new Sequential(CSVSource, offsetMap, hasHeaders);
    }
    public HeaderIndexed IndexBy(string[] indexByColumns) //headers implicitly required
    {
        var offsetMap = IndexByColumns(indexByColumns);
        return new HeaderIndexed(CSVSource, indexByColumns, offsetMap);
    }
    public ColumnIndexed IndexBy(int[] indexByColumns, bool hasHeaders = true)
    {
        var offsetMap = IndexByColumns(indexByColumns);
        return new ColumnIndexed(CSVSource, indexByColumns, offsetMap, hasHeaders);
    }
    private Encoding Encoding => CSVSource.Encoding;
    private string FilePath => CSVSource.FilePath;
    private string LineTerminator => CSVSource.LineTerminator;
    internal Dictionary<StringArrayKey, long> IndexByColumns(string[] columnHeadersToIndex)
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
                    if(DetectedDuplicateKeys == false && RowPositions.ContainsKey(key))
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
    internal Dictionary<StringArrayKey, long> IndexByColumns(int[] columnIndexesToIndex, bool hasHeader = true)
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
    internal Dictionary<int, long> Index(bool hasHeader = true)
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

