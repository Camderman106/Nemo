using Nemo.Model;
using System.Diagnostics;
namespace Nemo.IO.CSV;
public class Table
{
    public CSVSource CSVSource { get; private set; } = null!;
    public static Table From(CSVSource cSVSource)
    {
        return new Table() { CSVSource = cSVSource };
    }
    public class Sequential : IDisposable
    {
        public void Dispose()
        {
            RowParser?.Dispose();
            RowStream?.Dispose();
        }
        internal CSVSource CSVSource { get; private set; }
        internal IReadOnlyDictionary<int, long> OffsetMap { get; init; }
        internal IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; }
        public bool HasHeaders { get; init; }
        private int ColumnCount { get; init; }
        private readonly CsvRowStream RowStream;
        private readonly CsvRowParser RowParser;
        public Sequential(CSVSource source, IReadOnlyDictionary<int, long> offsetMap, bool hasHeaders)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            HasHeaders = hasHeaders;
            RowStream = new CsvRowStream(source);
            RowParser = new CsvRowParser(validateRows: true);

            RowStream.GetLine(out var row);
            RowParser.Parse(row);
            ColumnCount = RowParser.FieldCount;
            string[] headers;
            if (hasHeaders)
            {
                headers = RowParser.ToArray();
            }
            else
            {
                headers = Enumerable.Range(1, ColumnCount).Select(i => i.ToString()).ToArray();
            }

            ColumnIndexMap = Enumerable.Range(0, ColumnCount).Select(x => new KeyValuePair<string, int>(headers[x], x)).ToDictionary();
            RowStream.Seek(0);
        }
        #region Sequential.Lookups
        public string LookupString(int index, string returnColumn)
        {
            if (!HasHeaders) throw new LookupException("Cannot return by column name for file without headers");
            if (OffsetMap.TryGetValue(index, out long offset))
            {
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(ColumnIndexMap[returnColumn]).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(ColumnIndexMap[returnColumn]).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(returnColumn).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(returnColumn));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(returnColumn));
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
            RowParser?.Dispose();
            RowStream?.Dispose();
        }
        internal CSVSource CSVSource { get; private set; }
        internal IReadOnlyDictionary<StringArrayKey, long> OffsetMap { get; init; }
        internal IReadOnlyDictionary<string, int> ColumnIndexMap { get; init; }
        private int ColumnCount { get; init; }
        private readonly CsvRowStream RowStream;
        private readonly CsvRowParser RowParser;
        public ColumnIndexed(CSVSource source, IReadOnlyDictionary<StringArrayKey, long> offsetMap)
        {
            CSVSource = source;
            OffsetMap = offsetMap;
            RowStream = new CsvRowStream(source);
            RowParser = new CsvRowParser(validateRows: true);

            RowStream.GetLine(out var row);
            RowParser.Parse(row);
            ColumnCount = RowParser.FieldCount;
            string[] headers = RowParser.ToArray();

            ColumnIndexMap = Enumerable.Range(0, ColumnCount).Select(x => new KeyValuePair<string, int>(headers[x], x)).ToDictionary();
            RowStream.Seek(0);
        }
        #region ColumnIndexed.Lookups
        internal double? LookupTrace(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                if(!ColumnIndexMap.ContainsKey(returnColumn)) return null;
                var span = RowParser.GetField(ColumnIndexMap[returnColumn]);
                if (span.IsEmpty) return ModelBase.ZeroNoAverage;
                return double.Parse(span);
            }
            else
            {
                throw new LookupException($"Lookup not found in {Path.GetFileNameWithoutExtension(CSVSource.OriginalPath)} with key: {key}");
            }
        }

        public string LookupString(string[] lookupColumnValues, string returnColumn)
        {
            StringArrayKey key = new StringArrayKey(lookupColumnValues);
            if (OffsetMap.TryGetValue(key, out long offset))
            {
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(ColumnIndexMap[returnColumn]).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(ColumnIndexMap[returnColumn]).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(returnColumnIndex).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return RowParser.GetField(returnColumnIndex).ToString();
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(returnColumnIndex));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return double.Parse(RowParser.GetField(returnColumnIndex));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(ColumnIndexMap[returnColumn]));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(returnColumnIndex));
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
                RowStream.Seek(offset);
                RowStream.GetLine(out var row);
                RowParser.Parse(row);
                return int.Parse(RowParser.GetField(returnColumnIndex));
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
        Dictionary<StringArrayKey, long> RowPositions = new();
        using CsvRowStream rowStream = new CsvRowStream(CSVSource);
        using CsvRowParser rowParser = new CsvRowParser(validateRows: true);
        rowStream.GetLine(out var row);
        rowParser.Parse(row);
        var headers = rowParser.ToArray().ToList();
        int[] indexes = columnHeadersToIndex.Select(x => headers.IndexOf(x)).ToArray();
        Debug.Assert(indexes.All(x => x >= 0));
        while (rowStream.GetLine(out row))
        {
            rowParser.Parse(row);
            string[] keyvalues = new string[columnHeadersToIndex.Length];
            for (int i = 0; i < indexes.Length; i++)
            {
                keyvalues[i] = rowParser[indexes[i]].ToString();
            }
            var arrayKey = new StringArrayKey(keyvalues);
            RowPositions[arrayKey] = rowStream.BytePosOfCurrLine;
        }
        return RowPositions;
    }
    internal IReadOnlyDictionary<StringArrayKey, long> CreateIndexByColumns(int[] columnIndexesToIndex, bool hasHeader = true)
    {
        Dictionary<StringArrayKey, long> RowPositions = new();
        using CsvRowStream rowStream = new CsvRowStream(CSVSource);
        using CsvRowParser rowParser = new CsvRowParser(validateRows: true);
        rowStream.GetLine(out var row);
        rowParser.Parse(row);
        if (!hasHeader)
        {
            rowStream.Seek(0);//if there is no header then we need to move back to the top
        }
        while (rowStream.GetLine(out row))
        {
            rowParser.Parse(row);
            string[] keyvalues = new string[columnIndexesToIndex.Length];
            for (int i = 0; i < columnIndexesToIndex.Length; i++)
            {
                keyvalues[i] = rowParser[columnIndexesToIndex[i]].ToString();
            }
            var arrayKey = new StringArrayKey(keyvalues);
            RowPositions[arrayKey] = rowStream.BytePosOfCurrLine;
        }
        return RowPositions;

    }
    internal IReadOnlyDictionary<int, long> CreateIndexByRowIndex(bool hasHeader = true)
    {
        Dictionary<int, long> RowPositions = new Dictionary<int, long>();
        using CsvRowStream rowStream = new CsvRowStream(CSVSource);
        using CsvRowParser rowParser = new CsvRowParser(validateRows: true);
        rowStream.GetLine(out var row);
        rowParser.Parse(row);
        if (!hasHeader)
        {
            rowStream.Seek(0);//if there is no header then we need to move back to the top
        }
        int rowIndex = 0;
        while (rowStream.GetLine(out row))
        {
            rowIndex++;
            RowPositions[rowIndex] = rowStream.BytePosOfCurrLine;
        }
        return RowPositions;

    }

    #endregion

    #region Enumeration
    public IEnumerable<TableRecord> AsRecords()
    {
        using CsvRowStream rowStream = new CsvRowStream(CSVSource);
        using CsvRowParser rowParser = new CsvRowParser(validateRows: true);
        rowStream.GetLine(out var row);
        rowParser.Parse(row);

        string[] header = rowParser.ToArray();
        IReadOnlyDictionary<string, int> headerIndex = header.Zip(Enumerable.Range(0, header.Length)).ToDictionary(x => x.First, x => x.Second);
        int rowIndex = 0;
        bool morerows = true;
        while (morerows)
        {
            morerows = rowStream.GetLine(out row);
            if (!morerows && row.IsEmpty) 
                break; //filter out an empty final row
            rowIndex++;
            rowParser.Parse(row);
            yield return new TableRecord(rowIndex, rowParser.ToArray(), headerIndex);
        }
    }
    public IEnumerable<TableRow> AsRows(bool hasHeaders = false)
    {
        using CsvRowStream rowStream = new CsvRowStream(CSVSource);
        using CsvRowParser rowParser = new CsvRowParser(validateRows: true);
        rowStream.GetLine(out var row);
        rowParser.Parse(row);
        if (!hasHeaders) rowStream.Seek(0);
        int rowIndex = 0;
        while (rowStream.GetLine(out row))
        {
            rowIndex++;
            rowParser.Parse(row);
            yield return new TableRow(rowIndex, rowParser.ToArray());
        }
    }
    #endregion
    /// <summary>
    /// A wrapper for an int that enables the column Indexes to be indexed from 1. Literally just subtracts 1 from the index on construction
    /// </summary>
    public readonly struct ColumnIndex
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