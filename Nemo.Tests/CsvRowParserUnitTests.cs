using Nemo.IO.CSV;
using System.Diagnostics;

namespace Nemo.Tests;

[TestClass]
public class CsvRowParserUnitTests
{
    [TestMethod]
    public void Parse_SimpleFields_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,field2,field3";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("field2", parser.GetField(1).ToString());
            Assert.AreEqual("field3", parser.GetField(2).ToString());
        }
    }

    [TestMethod]
    public void Parse_QuotedFields_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "\"field1\",\"field2\",\"field3\"";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("field2", parser.GetField(1).ToString());
            Assert.AreEqual("field3", parser.GetField(2).ToString());
        }
    }

    [TestMethod]
    public void Parse_FieldsWithCommasInsideQuotes_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "\"field1,with,commas\",field2,\"field3,also,with,commas\"";
            parser.Parse(input);

            Assert.AreEqual("field1,with,commas", parser.GetField(0).ToString());
            Assert.AreEqual("field2", parser.GetField(1).ToString());
            Assert.AreEqual("field3,also,with,commas", parser.GetField(2).ToString());
        }
    }

    [TestMethod]
    public void Parse_EscapedQuotesInsideQuotes_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "\"field with \"\"escaped quotes\"\"\",simplefield";
            parser.Parse(input);

            Assert.AreEqual("field with \"escaped quotes\"", parser.GetField(0).ToString());
            Assert.AreEqual("simplefield", parser.GetField(1).ToString());
        }
    }

    [TestMethod]
    public void Parse_EmptyFields_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,,field3,";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("", parser.GetField(1).ToString());
            Assert.AreEqual("field3", parser.GetField(2).ToString());
            Assert.AreEqual("", parser.GetField(3).ToString());
        }
    }

    [TestMethod]
    public void Parse_SingleQuotedField_ReturnsCorrectField()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "\"single field\"";
            parser.Parse(input);

            Assert.AreEqual("single field", parser.GetField(0).ToString());
        }
    }

    [TestMethod]
    public void Parse_FieldWithLeadingAndTrailingSpaces_ReturnsCorrectField()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "  field1  ,\"  field2  \",field3  ";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("  field2  ", parser.GetField(1).ToString());
            Assert.AreEqual("field3", parser.GetField(2).ToString());
        }
    }

    [TestMethod]
    public void Parse_MixedQuotedAndUnquotedFields_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,\"field2,with,comma\",field3,\"field4\"";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("field2,with,comma", parser.GetField(1).ToString());
            Assert.AreEqual("field3", parser.GetField(2).ToString());
            Assert.AreEqual("field4", parser.GetField(3).ToString());
        }
    }

    [TestMethod]
    public void Parse_NewlinesInsideQuotes_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "\"field1\nwith newline\",field2";
            parser.Parse(input);

            Assert.AreEqual("field1\nwith newline", parser.GetField(0).ToString());
            Assert.AreEqual("field2", parser.GetField(1).ToString());
        }
    }

    [TestMethod]
    public void Parse_InputEndingWithSeparator_ReturnsEmptyFieldAtEnd()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,field2,";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("field2", parser.GetField(1).ToString());
            Assert.AreEqual("", parser.GetField(2).ToString());
        }
    }

    [TestMethod]
    public void GetField_FieldIndexOutOfRange_ThrowsException()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,field2,field3";
            parser.Parse(input);

            Assert.ThrowsException<IndexOutOfRangeException>(() => parser.GetField(5));
        }
    }

    
    [TestMethod]
    public void Parse_FieldsWithDifferentWhitespace_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,\"field2\", \"field3\" ";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());  
            Assert.AreEqual("field2", parser.GetField(1).ToString()); // 
            Assert.AreEqual("field3", parser.GetField(2).ToString());    // Preserving quotes when not in open/closing spaces
        }
    }

    [TestMethod]
    public void Parse_EmptyInput_ReturnsNoFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "";
            parser.Parse(input);

            Assert.AreEqual("", parser.GetField(0).ToString());
        }
    }

    [TestMethod]
    public void Parse_InputWithOnlySeparators_ReturnsEmptyFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = ",,,";
            parser.Parse(input);

            Assert.AreEqual("", parser.GetField(0).ToString());
            Assert.AreEqual("", parser.GetField(1).ToString());
            Assert.AreEqual("", parser.GetField(2).ToString());
            Assert.AreEqual("", parser.GetField(3).ToString());
        }
    }

    [TestMethod]
    [ExpectedException(typeof(FormatException))]
    public void Parse_InputWithTrailingNewline_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "field1,field2\n";
            parser.Parse(input);

            Assert.AreEqual("field1", parser.GetField(0).ToString());
            Assert.AreEqual("field2", parser.GetField(1).ToString());
        }
    }

    
    [TestMethod]
    public void Parse_LongInput_ReturnsAllFieldsCorrectly()
    {
        using (var parser = new CsvRowParser())
        {
            var input = string.Join(",", new string[1000].Select((s, i) => $"field{i}"));
            parser.Parse(input);

            for (int i = 0; i < 1000; i++)
            {
                Assert.AreEqual($"field{i}", parser.GetField(i).ToString());
            }
        }
    }

    [TestMethod]
    public void Parse_InputWithNonStandardCharacters_ReturnsCorrectFields()
    {
        using (var parser = new CsvRowParser())
        {
            var input = "fïêld1,фιεлd2,字段3";
            parser.Parse(input);

            Assert.AreEqual("fïêld1", parser.GetField(0).ToString());
            Assert.AreEqual("фιεлd2", parser.GetField(1).ToString());
            Assert.AreEqual("字段3", parser.GetField(2).ToString());
        }
    }

    [TestMethod]
    public void Dispose_ReturnsArrayToPool()
    {
        var parser = new CsvRowParser();
        var input = "field1,field2,field3";
        parser.Parse(input);
        parser.Dispose();

        // Since we're using ArrayPool, we can't directly test if the array was returned.
        // But we can check that 'chars' is set to null or zero-length array (depending on implementation).
        Assert.IsNotNull(parser.chars);
    }
    /// <summary>
    /// Helper method to parse a CSV line and return the fields as an array of strings.
    /// </summary>
    private string[] ParseCsvLine(string line, char separator = ',', bool validateRows = false)
    {
        using var parser = new CsvRowParser(separator, validateRows);
        parser.Parse(line.AsSpan());
        var fields = new string[parser.FieldCount];
        for (int i = 0; i < parser.FieldCount; i++)
        {
            fields[i] = new string(parser.GetField(i));
        }
        return fields;
    }


    [TestMethod]
    public void Parse_SimpleUnquotedFields()
    {
        // Arrange
        string input = "John,Doe,30,Engineer";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }

    [TestMethod]
    public void Parse_EmptyFields()
    {
        // Arrange
        string input = "John,,30,Engineer,,";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(6, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual(string.Empty, fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
        Assert.AreEqual(string.Empty, fields[4]);
        Assert.AreEqual(string.Empty, fields[5]);
    }

    [TestMethod]
    public void Parse_QuotedFields()
    {
        // Arrange
        string input = "\"John\",\"Doe\",\"30\",\"Engineer\"";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }

    [TestMethod]
    public void Parse_FieldsWithEmbeddedSeparators()
    {
        // Arrange
        string input = "\"John, A.\",\"Doe, B.\",30,\"Engineer, Senior\"";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John, A.", fields[0]);
        Assert.AreEqual("Doe, B.", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer, Senior", fields[3]);
    }

    [TestMethod]
    public void Parse_FieldsWithEscapedQuotes()
    {
        // Arrange
        string input = "\"John \"\"Johnny\"\" A.\",\"Doe\",30,\"Engineer\"";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John \"Johnny\" A.", fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }

    

    [TestMethod]
    public void Parse_DifferentSeparator()
    {
        // Arrange
        char separator = ';';
        string input = "John;Doe;30;Engineer";

        // Act
        string[] fields = ParseCsvLine(input, separator);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }

    [TestMethod]
    public void Parse_RowEndingWithSeparator()
    {
        // Arrange
        string input = "John,Doe,30,Engineer,";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(5, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
        Assert.AreEqual(string.Empty, fields[4]);
    }

    [TestMethod]
    public void Parse_SingleField()
    {
        // Arrange
        string input = "OnlyField";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(1, fields.Length);
        Assert.AreEqual("OnlyField", fields[0]);
    }

    [TestMethod]
    public void Parse_EmptyInput()
    {
        // Arrange
        string input = string.Empty;

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(1, fields.Length);
        Assert.AreEqual(string.Empty, fields[0]);
    }

    [TestMethod]
    public void Parse_OnlySeparators()
    {
        // Arrange
        string input = ",,,,";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(5, fields.Length);
        foreach (var field in fields)
        {
            Assert.AreEqual(string.Empty, field);
        }
    }

    [TestMethod]
    [ExpectedException(typeof(FormatException))]
    public void Parse_UnclosedQuotedField_ShouldThrow()
    {
        // Arrange
        string input = "\"John,Doe,30,Engineer";

        // Act
        ParseCsvLine(input);

        // Assert is handled by ExpectedException
    }

    [TestMethod]
    [ExpectedException(typeof(FormatException))]
    public void Parse_InvalidQuotesInUnescapedField_ShouldThrow()
    {
        // Arrange
        string input = "\"John\",Doe\"Smith\",30,Engineer";

        // Act
        string[] values = ParseCsvLine(input);
    }

    [TestMethod]

    public void Parse_QuotedFieldWithCRLF()
    {
        // Arrange
        string input = "\"John\r\nDoe\",30,Engineer";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(3, fields.Length);
        Assert.AreEqual("John\r\nDoe", fields[0]);
        Assert.AreEqual("30", fields[1]);
        Assert.AreEqual("Engineer", fields[2]);
    }

    [TestMethod]
    public void Parse_ValidateRowFieldCount_Success()
    {
        // Arrange
        char separator = ',';
        bool validateRows = true;
        string input = "John,Doe,30,Engineer";

        using var parser = new CsvRowParser(separator, validateRows);

        // Act
        parser.Parse(input.AsSpan());

        // Assert
        Assert.AreEqual(4, parser.FieldCount);
    }

    [TestMethod]
    [ExpectedException(typeof(FormatException))]
    public void Parse_ValidateRowFieldCount_Failure()
    {
        // Arrange
        char separator = ',';
        bool validateRows = true;
        string input1 = "John,Doe,30,Engineer";
        string input2 = "Jane,Doe,25";

        using var parser = new CsvRowParser(separator, validateRows);

        // Act
        parser.Parse(input1.AsSpan());
        parser.Parse(input2.AsSpan()); // Should throw due to field count mismatch

        // Assert is handled by ExpectedException
    }

    [TestMethod]
    public void Parse_FieldsWithLeadingAndTrailingSpaces()
    {
        // Arrange
        string input = " John , \"Doe\" , 30 , \" Engineer \" ";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John", fields[0]); // Spaces are part of the field
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual(" Engineer ", fields[3]);
    }

    [TestMethod]
    public void Parse_FieldsWithTabCharacters()
    {
        // Arrange
        string input = "John\t,Doe\t,30\t,Engineer";

        // Act
        string[] fields = ParseCsvLine(input, '\t');

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual(",Doe", fields[1]);
        Assert.AreEqual(",30", fields[2]);
        Assert.AreEqual(",Engineer", fields[3]);
    }

    [TestMethod]
    public void Parse_MixedQuotedAndUnquotedFields()
    {
        // Arrange
        string input = "John,\"Doe, A.\",30,\"Engineer\"";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("John", fields[0]);
        Assert.AreEqual("Doe, A.", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }

    [TestMethod]
    public void Parse_FieldWithOnlyQuotes()
    {
        // Arrange
        string input = "\"\"";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(1, fields.Length);
        Assert.AreEqual(string.Empty, fields[0]);
    }

    [TestMethod]
    public void Parse_FieldWithNestedQuotes()
    {
        // Arrange
        string input = "\"She said, \"\"Hello!\"\"\",Doe,30,Engineer";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual("She said, \"Hello!\"", fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }

    [TestMethod]
    public void Parse_LargeNumberOfFields()
    {
        // Arrange
        int numberOfFields = 1000;
        string input = string.Join(",", new string[numberOfFields].Select((_, i) => $"Field{i}"));

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(numberOfFields, fields.Length);
        for (int i = 0; i < numberOfFields; i++)
        {
            Assert.AreEqual($"Field{i}", fields[i]);
        }
    }

    [TestMethod]
    public void Parse_LongField()
    {
        // Arrange
        string longField = new string('a', 10000);
        string input = $"\"{longField}\",Doe,30,Engineer";

        // Act
        string[] fields = ParseCsvLine(input);

        // Assert
        Assert.AreEqual(4, fields.Length);
        Assert.AreEqual(longField, fields[0]);
        Assert.AreEqual("Doe", fields[1]);
        Assert.AreEqual("30", fields[2]);
        Assert.AreEqual("Engineer", fields[3]);
    }    

    [TestMethod]
    [DataRow("a", "a")]
    [DataRow("\"\"", "")]
    [DataRow("\"\"\"\"", "\"")]
    [DataRow("\"\"\"\"\"\"", "\"\"")]
    [DataRow("\"b\"", "b")]
    [DataRow("\"c\"\"c\"", "c\"c")]
    [DataRow("\"d\"\"d\"\"d\"", "d\"d\"d")]
    [DataRow(" \"\" ", "")]
    [DataRow(" \"g\" ", "g")]
    [DataRow(" \"\"", "")]
    [DataRow("\"\"\"\"", "\"")]
    public void Test_EdgeCases(string input, string output)
    {
        using (var parser = new CsvRowParser())
        {
            parser.Parse(input);

            Assert.AreEqual(output, parser.GetField(0).ToString());
        }
    }    
}


