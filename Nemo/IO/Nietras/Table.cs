using nietras.SeparatedValues;
using System.Diagnostics;
using System.Text;
namespace Nemo.IO.Nietras;

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
    const int LookupBufferSize = 512;
    public class Sequential : IDisposable
    {
        public void Dispose()
        {
            StreamReader?.Dispose();
            SepReader?.Dispose();
        }
        internal CSVSource CSVSource { get; private set; }
        internal IReadOnlyDictionary<int, long> OffsetMap { get; init; }
        internal IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; }
        public bool HasHeaders { get; init; }
        private int ColumnCount { get; init; }
        private StreamReader StreamReader;
        private SepReader SepReader;
        public Sequential(CSVSource source, IReadOnlyDictionary<int, long> offsetMap, bool hasHeaders)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            HasHeaders = hasHeaders;
            StreamReader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, LookupBufferSize));
            SepReader = Sep.Auto.Reader(o => (o with { HasHeader = hasHeaders })).From(StreamReader);
            ColumnIndexMap = Enumerable.Range(0, SepReader.Header.ColNames.Count).Select(x => new KeyValuePair<string, int>(SepReader.Header.ColNames[x], x)).ToDictionary();
            ColumnCount = SepReader.Header.ColNames.Count;
            StreamReader.BaseStream.Seek(0, SeekOrigin.Begin);
        }
        #region Sequential.Lookups
        public string LookupString(int index, string returnColumn)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(HasHeaders && ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with index: {index}");
            }
        }
        public string LookupString(int index, string returnColumn, Func<string> Fallback)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(HasHeaders && ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                return Fallback();
            }
        }
        public double LookupDouble(int index, string returnColumn)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(HasHeaders && ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with index: {index}");
            }
        }
        public double LookupDouble(int index, string returnColumn, Func<double> Fallback)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(HasHeaders && ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                return Fallback();
            }
        }
        public int LookupInt(int index, string returnColumn)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(HasHeaders && ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with index: {index}");
            }
        }
        public int LookupInt(int index, string returnColumn, Func<int> Fallback)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(HasHeaders && ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                return Fallback();
            }
        }
        public string LookupString(int index, ColumnIndex returnColumn)
        {
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(!HasHeaders || ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[returnColumn].Span.ToString();
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with index: {index}");
            }
        }

        public double LookupDouble(int index, ColumnIndex returnColumn)
        {
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(!HasHeaders || ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[returnColumn].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with index: {index}");
            }
        }

        public int LookupInt(int index, ColumnIndex returnColumn)
        {
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(!HasHeaders || ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[returnColumn].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with index: {index}");
            }
        }
        #endregion
    }
    public class ColumnIndexed : IDisposable
    {
        public void Dispose()
        {
            StreamReader?.Dispose();
            SepReader?.Dispose();
        }
        internal CSVSource CSVSource { get; private set; }
        internal IReadOnlyDictionary<StringArrayKey, long> OffsetMap { get; init; }
        internal IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; }
        private int ColumnCount { get; init; }
        private StreamReader StreamReader;
        private SepReader SepReader;
        public ColumnIndexed(CSVSource source, IReadOnlyDictionary<StringArrayKey, long> offsetMap)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            StreamReader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, LookupBufferSize));
            StreamReader.BaseStream.Seek(0, SeekOrigin.Begin);
            SepReader = Sep.Auto.Reader(o => (o with { HasHeader = true })).From(StreamReader);
            ColumnIndexMap = Enumerable.Range(0, SepReader.Header.ColNames.Count).Select(x => new KeyValuePair<string, int>(SepReader.Header.ColNames[x], x)).ToDictionary();
            ColumnCount = SepReader.Header.ColNames.Count;
        }
        #region ColumnIndexed.Lookups
        public string LookupString(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }
        public string LookupString(string[] lookupColumnValues, string returnColumn, Func<string> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.MoveNext();
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[ColumnIndexMap[returnColumn]].Span.ToString();
            }
            else
            {
                return Fallback();
            }
        }
        public string LookupString(string[] lookupColumnValues, ColumnIndex returnColumnIndex)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[returnColumnIndex].Span.ToString();
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }
        public string LookupString(string[] lookupColumnValues, ColumnIndex returnColumnIndex, Func<string> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return SepReader.Current[returnColumnIndex].Span.ToString();
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
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }
        public double LookupDouble(string[] lookupColumnValues, string returnColumn, Func<double> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                return Fallback();
            }
        }
        public double LookupDouble(string[] lookupColumnValues, ColumnIndex returnColumnIndex)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[returnColumnIndex].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }
        public double LookupDouble(string[] lookupColumnValues, ColumnIndex returnColumnIndex, Func<double> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return double.Parse(SepReader.Current[returnColumnIndex].Span);
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
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }
        public int LookupInt(string[] lookupColumnValues, string returnColumn, Func<int> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[ColumnIndexMap[returnColumn]].Span);
            }
            else
            {
                return Fallback();
            }
        }
        public int LookupInt(string[] lookupColumnValues, ColumnIndex returnColumnIndex)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[returnColumnIndex].Span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }
        public int LookupInt(string[] lookupColumnValues, ColumnIndex returnColumnIndex, Func<int> Fallback)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                SepReader.Seek(offset);
                Debug.Assert(ColumnCount == SepReader.Current.ColCount);
                return int.Parse(SepReader.Current[returnColumnIndex].Span);
            }
            else
            {
                return Fallback();
            }
        }
        #endregion
    }

    public ColumnIndexed IndexByColumns(string[] indexByColumns) //headers implicitly required
    {
        var offsetMap = CreateIndexByColumns(indexByColumns);
        return new ColumnIndexed(CSVSource, offsetMap);
    }
    public ColumnIndexed IndexByColumns(ColumnIndex[] indexByColumns)
    {
        var offsetMap = CreateIndexByColumns(indexByColumns.Select(x => (int)x).ToArray());
        return new ColumnIndexed(CSVSource, offsetMap);
    }
    public Sequential IndexByRowIndex(bool hasHeader)
    {
        var offsetMap = CreateIndexByRowIndex(hasHeader);
        return new Sequential(CSVSource, offsetMap, hasHeader);
    }

    #region CreateIndexes
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
    internal IReadOnlyDictionary<int, long> CreateIndexByRowIndex(bool hasHeader = true)
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
                    while (reader.MoveNext())
                    {
                        SepReader.Row row = reader.Current;
                        RowPositions[row.RowIndex] = offset; //1 is always the first record
                        offset += Encoding.GetByteCount(row.Span) + Encoding.GetByteCount(LineTerminator);
                    }
                }
                else
                {
                    while (reader.MoveNext())
                    {
                        SepReader.Row row = reader.Current;
                        RowPositions[row.RowIndex + 1] = offset; //1 is always the first record
                        offset += Encoding.GetByteCount(row.Span) + Encoding.GetByteCount(LineTerminator);
                    }
                }
            }
        }
        return RowPositions;
    }
    #endregion

    #region Enumeration
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
    public IEnumerable<TableRow> AsRows(bool hasHeaders = false)
    {
        using var reader = new StreamReader(new FileStream(CSVSource.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
        using var sep = Sep.Auto.Reader(o => o with { HasHeader = hasHeaders }).From(reader);
        var header = sep.Header.ColNames;
        foreach (var row in sep)
        {
            yield return new TableRow(row.LineNumberFrom, row[sep.Header.ColNames].ParseToArray<string>());
        }
    }
    #endregion
    /// <summary>
    /// A wrapper for an int that enables the column Indexes to be indexed from 1. Literally just subtracts 1 from the index on construction
    /// </summary>
    public struct ColumnIndex
    {
        private readonly int _index;
        private ColumnIndex(int index)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index), "ColumnIndex must start at 1.");
            _index = index;
        }
        // Implicit conversion from int to ColumnIndex
        public static implicit operator ColumnIndex(int index) => new ColumnIndex(index - 1);
        // Implicit conversion from ColumnIndex to int
        public static implicit operator int(ColumnIndex columnIndex) => columnIndex._index;

        public override string ToString() => _index.ToString();
    }
}
