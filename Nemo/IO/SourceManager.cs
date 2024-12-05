using ClosedXML.Excel;
using nietras.SeparatedValues;
using System.Security.Cryptography;
using System.Text;
namespace Nemo.IO;

public class SourceManager
{
    private string DataSource;
    public CSVSource Data;
    public SourceManager SetDataSource(string csvFilePath)
    {
        csvFilePath = Path.GetFullPath(csvFilePath);
        if (csvFilePath is null) throw new ArgumentNullException(nameof(csvFilePath));
        if (!Path.Exists(csvFilePath)) throw new FileNotFoundException(csvFilePath);

        DataSource = csvFilePath;
        return this;
    }
    private Dictionary<string, string> CsvSourceData = new Dictionary<string, string>();
    public SourceManager AddCSVSource(string sourceNameId, string csvFilePath)
    {
        csvFilePath = Path.GetFullPath(csvFilePath);
        if (sourceNameId is null ) throw new ArgumentNullException(nameof(sourceNameId));
        if(csvFilePath is null ) throw new ArgumentNullException(nameof(csvFilePath));
        if(!Path.Exists(csvFilePath)) throw new FileNotFoundException(csvFilePath);

        CsvSourceData.Add(sourceNameId, csvFilePath);
        return this;
    }
    public SourceManager AddCSVSource(string sourceNameId, CSVSource csvSource)
    {
        if (sourceNameId is null) throw new ArgumentNullException(nameof(sourceNameId));
        if (csvSource is null) throw new ArgumentNullException(nameof(csvSource));
        if (!Path.Exists(csvSource.FilePath)) throw new FileNotFoundException(csvSource.FilePath);

        CsvSourceData.Add(sourceNameId, csvSource.FilePath);
        return this;
    }

    private Dictionary<string, ExcelRangeData> ExcelSourceData = new();
    public SourceManager AddExcelSource(string sourceNameId, string workbookPath, string? tabName = null, string? range = null)
    {
        workbookPath = Path.GetFullPath(workbookPath);
        if (sourceNameId is null) throw new ArgumentNullException(nameof(sourceNameId));
        if (workbookPath is null) throw new ArgumentNullException(nameof(workbookPath));
        if (!Path.Exists(workbookPath)) throw new FileNotFoundException(workbookPath);
        if(tabName is null && range is null) throw new ArgumentNullException("Must specifiy at least one of 'sheet name' and 'range'");

        ExcelSourceData[sourceNameId] = new ExcelRangeData(workbookPath, tabName, range);
        return this;
    }

    public IDictionary<string, CSVSource> Tables;
    internal void ExtractSources(string extractionDirectory)
    {
        extractionDirectory = Path.GetFullPath(extractionDirectory);
        Directory.CreateDirectory(extractionDirectory);

        Dictionary<string, CSVSource> result = new Dictionary<string, CSVSource>();
        if (DataSource is not null)
        {
            var original = Path.GetFullPath(DataSource);
            string extracted = Path.GetFullPath(Path.Combine(extractionDirectory, ComputeHashHex(original) + ".tbl"));
            if (File.Exists(extracted))
            {
                Data = new CSVSource(extracted, original);
            }
            else
            {
                Data = new CSVSource(new FileInfo(original).CopyTo(extracted).FullName, original);
            }
        }

        foreach(var source in CsvSourceData)
        {
            var original = Path.GetFullPath(source.Value);
            string extracted = Path.GetFullPath(Path.Combine(extractionDirectory, ComputeHashHex(original) + ".tbl"));            
            if (File.Exists(extracted))
            {
                result[source.Key] = new CSVSource(extracted, original);
            }
            else
            {
                result[source.Key] = new CSVSource(new FileInfo(original).CopyTo(extracted).FullName, original);
            }
        }
        foreach (var sourceGroup in ExcelSourceData.GroupBy(x => x.Value.FilePath))
        {
            var original = Path.GetFullPath(sourceGroup.Key);
            
            using (var workbook = new XLWorkbook(original))
            {
                foreach (var source in sourceGroup)
                {
                    string extracted = Path.GetFullPath(Path.Combine(extractionDirectory, ComputeHashHex(source.Value.ToString()) + ".tbl"));
                    if (File.Exists(extracted))
                    {
                        result[source.Key] = new CSVSource(extracted, original);
                    }
                    else
                    {
                        var range = source.Value;
                        if (range.SheetName is null && range.Range is not null)
                        {
                            //assume named range
                            var namedRange = workbook.DefinedName(range.Range);
                            if (namedRange != null)
                            {
                                ExtractRangeToCsv(namedRange.Ranges.First(), extracted); // If it's a named range, use it.
                            }
                            else
                            {
                                Console.WriteLine($"Named range not found. {range.ToString()}");
                                continue;
                            }
                        }
                        else
                        {
                            var worksheet = workbook.Worksheet(range.SheetName);
                            if (range.Range is null)
                            {
                                ExtractWorksheetToCsv(worksheet, extracted);
                            }
                            else
                            {
                                ExtractRangeToCsv(worksheet.Range(range.Range), extracted);
                            }
                        }
                        result[source.Key] = new CSVSource(extracted, original);
                    }
                }
            }
        }
        Tables = result;
    }
    private void ExtractWorksheetToCsv(IXLWorksheet worksheet, string outputFile)
    {
        // Prepare the CSV writer using Sep
        using (var writer = Sep.New(',').Writer(o => o with { WriteHeader = false }).ToFile(outputFile))
        {
            foreach (var row in worksheet.RangeUsed().Rows())
            {
                using (var outrow = writer.NewRow())
                {
                    for (int i = 0; i < row.CellCount(); i++)
                    {
                        outrow[i].Set(GetCellValueAsString(row.Cell(i + 1)));
                    }
                }
            }
        }
    }

    private void ExtractRangeToCsv(IXLRange range, string outputFile)
    {
        // Prepare the CSV writer using Sep
        using (var writer = Sep.New(',').Writer(o => o with { WriteHeader = false }).ToFile(outputFile))
        {
            foreach (var row in range.Rows())
            {
                using (var outrow = writer.NewRow())
                {
                    for (int i = 0; i < row.CellCount(); i++)
                    {
                        outrow[i].Set(GetCellValueAsString(row.Cell(i+1)));
                    }
                }
            }
        }
    }

    private string GetCellValueAsString(IXLCell cell)
    {
        if (cell == null || cell.IsEmpty())
            return "";

        return cell.Value.ToString();

    }
    private static string ComputeHashHex(string input)
    {
        using (var sha256 = SHA256.Create())
        {
            byte[] hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(input));
            return ConvertToHexString(hashBytes);
        }
    }

    // Converts a byte array to a hex string.
    private static string ConvertToHexString(byte[] bytes)
    {
        var stringBuilder = new StringBuilder(bytes.Length * 2);
        foreach (byte b in bytes)
        {
            stringBuilder.Append(b.ToString("x2"));
        }
        return stringBuilder.ToString();
    }
    
}
public record ExcelRangeData(string FilePath, string? SheetName, string? Range);
