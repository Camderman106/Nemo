using Nemo.IO.CSV;
using System.Text;

namespace Nemo.Tests;

[TestClass]
public class CsvRowStreamTests
{
    [TestMethod]
    public void TestEmptyFile()
    {
        using (var stream = new MemoryStream())
        using (var csvRowStream = new CsvRowStream(stream))
        {
            csvRowStream.GetLine(out var line);
            Assert.IsTrue(line.IsEmpty, "Line should be empty for an empty file.");
        }
    }

    [TestMethod]
    public void TestFileWithOnlyNewlines()
    {
        var content = "\n\n\n";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            for (int i = 0; i < 3; i++)
            {
                csvRowStream.GetLine(out var line);
                Assert.IsTrue(line.IsEmpty, $"Line {i} should be empty.");
            }
            csvRowStream.GetLine(out var endLine);    
            Assert.IsTrue(endLine.IsEmpty, "Should return empty span at EOF.");
        }
    }

    [TestMethod]
    public void TestSingleLineWithoutNewlineAtEnd()
    {
        var content = "This is a single line without a newline at the end.";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            csvRowStream.GetLine(out var line);
            Assert.AreEqual(content, line.ToString());
            csvRowStream.GetLine(out var endLine);
            Assert.IsTrue(endLine.IsEmpty, "Should return empty span at EOF.");
        }
    }

    [DataTestMethod]
    [DataRow("Line1\nLine2\nLine3")]
    [DataRow("Line1\r\nLine2\r\nLine3")]
    [DataRow("Line1\rLine2\rLine3")]
    public void TestDifferentNewlineCharacters(string content)
    {
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            var expectedLines = new[] { "Line1", "Line2", "Line3" };
            foreach (var expected in expectedLines)
            {
                csvRowStream.GetLine(out var line);
                Assert.AreEqual(expected, line.ToString());
            }
            csvRowStream.GetLine(out var endLine);
            Assert.IsTrue(endLine.IsEmpty, "Should return empty span at EOF.");
        }
    }

    [TestMethod]
    public void TestLargeLineExceedingBufferSize()
    {
        var largeLine = new string('A', 5000); // Exceeds initial buffer size
        var content = $"{largeLine}\nSecondLine";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            csvRowStream.GetLine(out var line1);
            Assert.AreEqual(largeLine, line1.ToString());
            csvRowStream.GetLine(out var line2);
            Assert.AreEqual("SecondLine", line2.ToString());
            csvRowStream.GetLine(out var endLine);
            Assert.IsTrue(endLine.IsEmpty, "Should return empty span at EOF.");
        }
    }

    [TestMethod]
    public void TestMultiByteCharacters()
    {
        var content = "Line with emoji 😀\nAnother line with emoji 😃";
        using (var stream = GenerateStreamFromString(content, Encoding.UTF8))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            csvRowStream.GetLine(out var line1);
            Assert.AreEqual("Line with emoji 😀", line1.ToString());
            csvRowStream.GetLine(out var line2);
            Assert.AreEqual("Another line with emoji 😃", line2.ToString());
            csvRowStream.GetLine(out var endLine);
            Assert.IsTrue(endLine.IsEmpty, "Should return empty span at EOF.");
        }
    }

    [TestMethod]
    public void TestSeekToStartOfFile()
    {
        var content = "First Line\nSecond Line\nThird Line";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            // Read first line
            csvRowStream.GetLine(out var line1);
            Assert.AreEqual(11, csvRowStream.BytePosOfNextLine);

            Assert.AreEqual("First Line", line1.ToString());

            // Seek to start
            csvRowStream.Seek(0);
            Assert.AreEqual(0, csvRowStream.BytePosOfNextLine);


            // Read first line again
            csvRowStream.GetLine(out var line1Again);
            Assert.AreEqual(11, csvRowStream.BytePosOfNextLine);

            Assert.AreEqual("First Line", line1Again.ToString());
        }
    }

    [TestMethod]
    public void TestSeekWithinCurrentBuffer()
    {
        var content = "Line1\nLine2\nLine3\nLine4";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            // Read first two lines
            Assert.AreEqual(0, csvRowStream.BytePosOfNextLine);
            csvRowStream.GetLine(out var _);
            Assert.AreEqual(6, csvRowStream.BytePosOfNextLine);
            csvRowStream.GetLine(out var _);
            Assert.AreEqual(12, csvRowStream.BytePosOfNextLine);

            // Get current stream position
            long currentPosition = csvRowStream.BytePosOfNextLine;

            // Read third line
            csvRowStream.GetLine(out var line3);
            Assert.AreEqual(18, csvRowStream.BytePosOfNextLine);
            Assert.AreEqual("Line3", line3.ToString());

            // Seek back to the position before reading third line
            csvRowStream.Seek(currentPosition);
            Assert.AreEqual(12, csvRowStream.BytePosOfNextLine);

            // Read third line again
            csvRowStream.GetLine(out var line3Again);
            Assert.AreEqual(18, csvRowStream.BytePosOfNextLine);
            Assert.AreEqual("Line3", line3Again.ToString());
        }
    }

    [TestMethod]
    public void TestSeekOutsideCurrentBuffer()
    {
        var content = new string('A', 2000) + "\nLineAfterLargeLine";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            // Read the large line
            Assert.AreEqual(0, csvRowStream.BytePosOfNextLine);
            csvRowStream.GetLine(out var a);
            Assert.AreEqual(2001, csvRowStream.BytePosOfNextLine);


            // Get position after large line
            //long positionAfterLargeLine = stream.Position;
            long positionAfterLargeLine = csvRowStream.BytePosOfNextLine;

            // Read next line
            csvRowStream.GetLine(out var line2);
            Assert.AreEqual(2019, csvRowStream.BytePosOfNextLine);
            Assert.AreEqual("LineAfterLargeLine", line2.ToString());

            // Seek back to position after large line
            csvRowStream.Seek(positionAfterLargeLine);

            // Read the line again
            csvRowStream.GetLine(out var line2Again);
            Assert.AreEqual("LineAfterLargeLine", line2Again.ToString());
        }
    }

    [TestMethod]
    public void TestEndOfFileHandling()
    {
        var content = "Last Line";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            csvRowStream.GetLine(out var line);
            Assert.AreEqual("Last Line", line.ToString());

            csvRowStream.GetLine(out var endLine);
            Assert.IsTrue(endLine.IsEmpty, "Should return empty span at EOF.");

            // Try reading again
            csvRowStream.GetLine(out var anotherEndLine);
            Assert.IsTrue(anotherEndLine.IsEmpty, "Should consistently return empty span at EOF.");
        }
    }

    [TestMethod]
    public void TestHandlingOfByteOrderMarks()
    {
        var content = "Line with BOM";
        var utf8Bom = new UTF8Encoding(true);
        using (var stream = GenerateStreamFromString(content, utf8Bom))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            csvRowStream.GetLine(out var line);
            Assert.AreEqual("Line with BOM", line.ToString());
        }
    }

    [TestMethod]
    public void TestRepeatedReadsAndSeeks()
    {
        var content = "Line1\nLine2\nLine3\nLine4\nLine5";
        using (var stream = GenerateStreamFromString(content))
        using (var csvRowStream = new CsvRowStream(stream))
        {
            // Read first two lines
            csvRowStream.GetLine(out var _);
            csvRowStream.GetLine(out var _);

            // Seek to start
            csvRowStream.Seek(0);

            // Read all lines
            var lines = new string[5];
            for (int i = 0; i < 5; i++)
            {
                csvRowStream.GetLine(out var line);
                lines[i] = line.ToString();
            }

            Assert.AreEqual("Line1", lines[0]);
            Assert.AreEqual("Line2", lines[1]);
            Assert.AreEqual("Line3", lines[2]);
            Assert.AreEqual("Line4", lines[3]);
            Assert.AreEqual("Line5", lines[4]);
        }
    }

    // Helper method
    private MemoryStream GenerateStreamFromString(string s, Encoding encoding = null)
    {
        encoding ??= Encoding.UTF8;
        return new MemoryStream(encoding.GetBytes(s));
    }
}


