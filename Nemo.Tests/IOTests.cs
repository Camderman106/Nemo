using Nemo.IO;
using System.Runtime.CompilerServices;

namespace Nemo.Tests;

[TestClass]
public class IOTests
{
    [TestMethod]
    public void CSVSource()
    {
        {
            string path1 = "FakeData.csv";
            CSVSource reader1 = new CSVSource(path1);
            var index1 = Table.From(reader1).IndexSequential();
            //Assert.AreEqual(3, index2[0]);
            Assert.AreEqual(51, index1.OffsetMap[1]);
            Assert.AreEqual(85, index1.OffsetMap[2]);
            Assert.AreEqual(118, index1.OffsetMap[3]);
            Assert.AreEqual(151, index1.OffsetMap[4]);
            Assert.AreEqual(184, index1.OffsetMap[5]);
        }
        {
            string path2 = "FakeDataNoBOM.csv";
            CSVSource reader2 = new CSVSource(path2);
            var index2 = Table.From(reader2).IndexSequential();
            //Assert.AreEqual(3 - 3, index2[0]);
            Assert.AreEqual(51 - 3, index2.OffsetMap[1]);
            Assert.AreEqual(85 - 3, index2.OffsetMap[2]);
            Assert.AreEqual(118 - 3, index2.OffsetMap[3]);
            Assert.AreEqual(151 - 3, index2.OffsetMap[4]);
            Assert.AreEqual(184 - 3, index2.OffsetMap[5]);
        }

        {
            string path1 = "FakeData.csv";
            CSVSource reader1 = new CSVSource(path1);
            var index1 = Table.From(reader1).IndexSequential(false);
            Assert.AreEqual(3, index1.OffsetMap[0]);
            Assert.AreEqual(51, index1.OffsetMap[1]);
            Assert.AreEqual(85, index1.OffsetMap[2]);
            Assert.AreEqual(118, index1.OffsetMap[3]);
            Assert.AreEqual(151, index1.OffsetMap[4]);
            Assert.AreEqual(184, index1.OffsetMap[5]);
        }
        {
            string path2 = "FakeDataNoBOM.csv";
            CSVSource reader2 = new CSVSource(path2);
            var index2 = Table.From(reader2).IndexSequential(false);
            Assert.AreEqual(3 - 3, index2.OffsetMap[0]);
            Assert.AreEqual(51 - 3, index2.OffsetMap[1]);
            Assert.AreEqual(85 - 3, index2.OffsetMap[2]);
            Assert.AreEqual(118 - 3, index2.OffsetMap[3]);
            Assert.AreEqual(151 - 3, index2.OffsetMap[4]);
            Assert.AreEqual(184 - 3, index2.OffsetMap[5]);
        }

        {
            string path = "FakeData.csv";
            CSVSource reader = new CSVSource(path);
            var index1 = Table.From(reader).CreateIndexByColumns(["POL_NO", "DOB"]).ToList();
            CollectionAssert.AreEqual(index1[0].Key.Strings, new string[] { "P0001", "30/04/1994" });
            Assert.AreEqual(51, index1[0].Value);

            CollectionAssert.AreEqual(index1[1].Key.Strings, new string[] { "P0002", "09/03/1969" });
            Assert.AreEqual(85, index1[1].Value);

            CollectionAssert.AreEqual(index1[2].Key.Strings, new string[] { "P0003", "20/10/1955" });
            Assert.AreEqual(118, index1[2].Value);

            CollectionAssert.AreEqual(index1[3].Key.Strings, new string[] { "P0004", "24/07/1976" });
            Assert.AreEqual(151, index1[3].Value);

            CollectionAssert.AreEqual(index1[4].Key.Strings, new string[] { "P0005", "05/10/1999" });
            Assert.AreEqual(184, index1[4].Value);

        }
        {
            string path2 = "FakeDataNoBOM.csv";
            CSVSource reader2 = new CSVSource(path2);
            var index2 = Table.From(reader2).CreateIndexByColumns(["POL_NO", "DOB"]).ToList();
            CollectionAssert.AreEqual(index2[0].Key.Strings, new string[] { "P0001", "30/04/1994" });
            Assert.AreEqual(51 - 3, index2[0].Value);

            CollectionAssert.AreEqual(index2[1].Key.Strings, new string[] { "P0002", "09/03/1969" });
            Assert.AreEqual(85 - 3, index2[1].Value);

            CollectionAssert.AreEqual(index2[2].Key.Strings, new string[] { "P0003", "20/10/1955" });
            Assert.AreEqual(118 - 3, index2[2].Value);

            CollectionAssert.AreEqual(index2[3].Key.Strings, new string[] { "P0004", "24/07/1976" });
            Assert.AreEqual(151 - 3, index2[3].Value);

            CollectionAssert.AreEqual(index2[4].Key.Strings, new string[] { "P0005", "05/10/1999" });
            Assert.AreEqual(184 - 3, index2[4].Value);
        }

        {
            string path = "FakeData.csv";
            CSVSource reader = new CSVSource(path);
            var index1 = Table.From(reader).CreateIndexByColumns([0,1]).ToList();
            CollectionAssert.AreEqual(index1[0].Key.Strings, new string[] { "P0001", "30/04/1994" });
            Assert.AreEqual(51, index1[0].Value);

            CollectionAssert.AreEqual(index1[1].Key.Strings, new string[] { "P0002", "09/03/1969" });
            Assert.AreEqual(85, index1[1].Value);

            CollectionAssert.AreEqual(index1[2].Key.Strings, new string[] { "P0003", "20/10/1955" });
            Assert.AreEqual(118, index1[2].Value);

            CollectionAssert.AreEqual(index1[3].Key.Strings, new string[] { "P0004", "24/07/1976" });
            Assert.AreEqual(151, index1[3].Value);

            CollectionAssert.AreEqual(index1[4].Key.Strings, new string[] { "P0005", "05/10/1999" });
            Assert.AreEqual(184, index1[4].Value);

        }
        {
            string path2 = "FakeDataNoBOM.csv";
            CSVSource reader2 = new CSVSource(path2);
            var index2 = Table.From(reader2).CreateIndexByColumns([0, 1]).ToList();
            CollectionAssert.AreEqual(index2[0].Key.Strings, new string[] { "P0001", "30/04/1994" });
            Assert.AreEqual(51 - 3, index2[0].Value);

            CollectionAssert.AreEqual(index2[1].Key.Strings, new string[] { "P0002", "09/03/1969" });
            Assert.AreEqual(85 - 3, index2[1].Value);

            CollectionAssert.AreEqual(index2[2].Key.Strings, new string[] { "P0003", "20/10/1955" });
            Assert.AreEqual(118 - 3, index2[2].Value);

            CollectionAssert.AreEqual(index2[3].Key.Strings, new string[] { "P0004", "24/07/1976" });
            Assert.AreEqual(151 - 3, index2[3].Value);

            CollectionAssert.AreEqual(index2[4].Key.Strings, new string[] { "P0005", "05/10/1999" });
            Assert.AreEqual(184 - 3, index2[4].Value);
        }

        {
            string path = "FakeData.csv";
            CSVSource reader = new CSVSource(path);
            var index1 = Table.From(reader).CreateIndexByColumns([0, 1], false).ToList();
            CollectionAssert.AreEqual(index1[0].Key.Strings, new string[] { "POL_NO", "DOB" });
            Assert.AreEqual(3, index1[0].Value);

            CollectionAssert.AreEqual(index1[1].Key.Strings, new string[] { "P0001", "30/04/1994" });
            Assert.AreEqual(51, index1[1].Value);

            CollectionAssert.AreEqual(index1[2].Key.Strings, new string[] { "P0002", "09/03/1969" });
            Assert.AreEqual(85, index1[2].Value);

            CollectionAssert.AreEqual(index1[3].Key.Strings, new string[] { "P0003", "20/10/1955" });
            Assert.AreEqual(118, index1[3].Value);

            CollectionAssert.AreEqual(index1[4].Key.Strings, new string[] { "P0004", "24/07/1976" });
            Assert.AreEqual(151, index1[4].Value);

            CollectionAssert.AreEqual(index1[5].Key.Strings, new string[] { "P0005", "05/10/1999" });
            Assert.AreEqual(184, index1[5].Value);

        }
        {
            string path2 = "FakeDataNoBOM.csv";
            CSVSource reader2 = new CSVSource(path2);
            var index2 = Table.From(reader2).CreateIndexByColumns([0, 1], false).ToList();
            CollectionAssert.AreEqual(index2[0].Key.Strings, new string[] { "POL_NO", "DOB" });
            Assert.AreEqual(3 - 3, index2[0].Value);

            CollectionAssert.AreEqual(index2[1].Key.Strings, new string[] { "P0001", "30/04/1994" });
            Assert.AreEqual(51 - 3, index2[1].Value);

            CollectionAssert.AreEqual(index2[2].Key.Strings, new string[] { "P0002", "09/03/1969" });
            Assert.AreEqual(85 - 3, index2[2].Value);

            CollectionAssert.AreEqual(index2[3].Key.Strings, new string[] { "P0003", "20/10/1955" });
            Assert.AreEqual(118 - 3, index2[3].Value);

            CollectionAssert.AreEqual(index2[4].Key.Strings, new string[] { "P0004", "24/07/1976" });
            Assert.AreEqual(151 - 3, index2[4].Value);

            CollectionAssert.AreEqual(index2[5].Key.Strings, new string[] { "P0005", "05/10/1999" });
            Assert.AreEqual(184 - 3, index2[5].Value);
        }
    }
    [TestMethod]
    public void TestLookups()
    {
        {
            string path = "FakeData.csv";
            CSVSource source = new CSVSource(path);
            var table = Table.From(source).IndexByColumns(["POL_NO"]);

            Assert.AreEqual("20/10/1955", table.LookupString(["P0003"], "DOB"));
            Assert.AreEqual(36, table.LookupDouble(["P0005"], "AGE_AT_ENTRY"));
            Assert.AreEqual(100000, table.LookupInt(["P0001"], "SUM_ASSURED"));

            Assert.ThrowsException<LookupException>(() => table.LookupString(["xxx"], "DOB"));

            Assert.AreEqual("Missing", table.LookupString(["xxx"], "DOB", () => "Missing"));
            Assert.AreEqual(-1, table.LookupDouble(["xxx"], "AGE_AT_ENTRY", () => -1d));
            Assert.AreEqual(-1, table.LookupInt(["xxx"], "SUM_ASSURED", () => -1));
        }
        {
            string path = "FakeDataNOBOM.csv";
            CSVSource source = new CSVSource(path);
            var table = Table.From(source).IndexByColumns(["POL_NO"]);

            Assert.AreEqual("20/10/1955", table.LookupString(["P0003"], "DOB"));
            Assert.AreEqual(36, table.LookupDouble(["P0005"], "AGE_AT_ENTRY"));
            Assert.AreEqual(100000, table.LookupInt(["P0001"], "SUM_ASSURED"));

            Assert.ThrowsException<LookupException>(() => table.LookupString(["xxx"], "DOB"));

            Assert.AreEqual("Missing", table.LookupString(["xxx"], "DOB", () => "Missing"));
            Assert.AreEqual(-1, table.LookupDouble(["xxx"], "AGE_AT_ENTRY", () => -1d));
            Assert.AreEqual(-1, table.LookupInt(["xxx"], "SUM_ASSURED", () => -1));
        }
    }

    [TestMethod]
    public void TestLookupsPerformance()
    {
        {
            string path = "FakeData.csv";
            CSVSource source = new CSVSource(path);
            var table = Table.From(source).IndexByColumns(["POL_NO"]);
            for (int i = 0; i < 1000; i++)
            {
                Assert.AreEqual("20/10/1955", table.LookupString(["P0003"], "DOB"));
                Assert.AreEqual(36, table.LookupDouble(["P0005"], "AGE_AT_ENTRY"));
                Assert.AreEqual(100000, table.LookupInt(["P0001"], "SUM_ASSURED"));
                Assert.AreEqual(30, table.LookupInt(["P0004"], "POL_TERM_Y"));
                Assert.AreEqual(50, table.LookupInt(["P0002"], "POL_TERM_Y"));
            }
            
        }
    }
}
