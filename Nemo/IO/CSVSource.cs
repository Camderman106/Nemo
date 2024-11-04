using System.Reflection.PortableExecutable;
using System.Text;
using System.Windows.Markup;
namespace Nemo.IO;

/// <summary>
/// Represent a memory mapped csv file. Designed to be fast as fuck whilst using fuck all memory
/// By default it will provide an IEnumerable<CSVRecord>
/// It has an option to index the file by a certain set of columns and allow you to seem through the file that way
/// It must be thread safe. So each thread will have its own stream
/// </summary>
public class CSVSource
{   public string FilePath { get; init; }
    internal Encoding Encoding { get; init; } = new UTF8Encoding(false);
    internal string LineTerminator { get; set; } = Environment.NewLine;
    public CSVSource(string filePath)
    {
        FilePath = filePath;

        //Detect the BOM properly
        var utf8nobom = new UTF8Encoding(false); //this prevents populating the preamble unless there actually is one. UTF8 has a mode with and without preamble
        using var stream = new FileStream(FilePath, FileMode.Open, FileAccess.Read);
        using var reader = new StreamReader(stream, utf8nobom, true);
        reader.Peek();
        Encoding = reader.CurrentEncoding;

        //Detect the first line delimiter 
        char previousChar = '\0';
        int currentCharInt;
        while ((currentCharInt = reader.Read()) >= 0)
        {
            char currentChar = (char)currentCharInt;

            if (currentChar == '\n')
            {
                if (previousChar == '\r')
                {
                    LineTerminator = "\r\n"; // Windows-style line separator
                    break;
                }
                else
                {
                    LineTerminator = "\n"; // Unix-style line separator
                    break;
                }
            }

            previousChar = currentChar;

            // Break the loop after a reasonable number of characters to prevent unnecessary reads
            if (reader.BaseStream.Position > 1024 * 256)
            {
                // If no line separator is found, default to Environment.NewLine or throw an exception
                throw new InvalidOperationException("Line separator could not be detected.");
            }
        }
    }    
}
        
