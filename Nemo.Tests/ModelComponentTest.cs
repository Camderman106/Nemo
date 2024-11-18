using Nemo.IO;
using Nemo.Model;
using Nemo.Model.Components;
namespace Nemo.Tests
{
    [TestClass]
    public sealed class ModelComponentTest
    {
        [TestMethod]
        public void ColumnTests()
        {
            Column column = new Column("test", 0, 10, AggregationMethod.Sum,(int t) => 10);
            column.Values = Enumerable.Repeat(5, 10).Select(x => new ColumnValue() { State = ColumnValueState.Calculated, Value = x }).ToArray();
            column.Values[5].State = ColumnValueState.Uncalculated;
            column.Values[6].State = ColumnValueState.ZeroNoAverage;
            Assert.AreEqual(5, column.At(4));
            Assert.AreEqual(10, column.At(5));
            Assert.AreEqual(0, column.At(6));
            (column as IModelComponent).Reset();
            Assert.AreEqual(10, column.At(4));
            Assert.AreEqual(10, column.At(5));
            Assert.AreEqual(10, column.At(6));
        }

        [TestMethod]
        public void ScalarTests_String()
        {
            Scalar<string> scalar = new("test", () => "Value");
            Assert.AreEqual(false, scalar.IsCalculated());
            {
                string @implicit = scalar;
            }
            Assert.AreEqual(true, scalar.IsCalculated());
            Assert.AreEqual("Value", scalar);
            scalar.Reset();
            Assert.AreEqual(false, scalar.IsCalculated());
            scalar.SetValue("Potato");
            Assert.AreEqual(true, scalar.IsCalculated());
            Assert.AreEqual("Potato", scalar);
            scalar.Reset();
            Assert.ThrowsException<ScalarException>(() => scalar.SetValue(null!));
        }
        [TestMethod]
        public void ScalarTests_Double()
        {
            Scalar<double> scalar = new("test", () => 5.0);
            Assert.AreEqual(false, scalar.IsCalculated());
            {
                double @implicit = scalar;
            }
            Assert.AreEqual(true, scalar.IsCalculated());
            Assert.AreEqual(5d, scalar);
            scalar.Reset();
            Assert.AreEqual(false, scalar.IsCalculated());
            scalar.SetValue(10d);
            Assert.AreEqual(true, scalar.IsCalculated());
            Assert.AreEqual(10d, scalar);
            scalar.Reset();
        }
        [TestMethod]
        public void ScalarTests_Int()
        {
            Scalar<int> scalar = new("test", () => 5);
            Assert.AreEqual(false, scalar.IsCalculated());
            {
                int @implicit = scalar;
            }
            Assert.AreEqual(true, scalar.IsCalculated());
            Assert.AreEqual(5, scalar);
            scalar.Reset();
            Assert.AreEqual(false, scalar.IsCalculated());
            scalar.SetValue(10);
            Assert.AreEqual(true, scalar.IsCalculated());
            Assert.AreEqual(10, scalar);
            scalar.Reset();
        }
        [TestMethod]
        public void ScalarTests_Other()
        {
            Assert.ThrowsException<ScalarException>(() => new Scalar<DateTime>("test", () => DateTime.Now));
        }
    }

    [TestClass]
    public sealed class ModelTest
    {
        class TestClass : ModelBase
        {
            internal Column column1;
            internal Column column2;
            public TestClass(Projection proj, OutputSet outputSet, string group) : base(proj, outputSet)
            {
                column1 = new Column("TestColumn", proj.T_Start, proj.T_End, AggregationMethod.Sum, (int t) => 2 * t + 1);
                column2 = new Column("TestColumn2", proj.T_Start, proj.T_End, AggregationMethod.Sum, (int t) => column1.At(t));
            }

        }

        [TestMethod]
        public void TestSimpleClass()
        {
            Projection proj = new Projection(0,0,1,1, new Dictionary<string, CSVSource>());
            TestClass testClass = new TestClass(proj, new OutputSet(), "group");
            testClass.InitialiseBuffer("testgroup");
            Assert.IsFalse(testClass.column2.IsCalculatedAt(0));
            testClass.Target();
            Assert.IsTrue(testClass.column2.IsCalculatedAt(0));
            Assert.AreEqual(3, testClass.column1.Peek(1));
            var span = testClass.column1.TrimForExport(0, 1);
            Assert.AreEqual(1, span.Length);
            Assert.AreEqual(1, span[0].Value);
            span = testClass.column1.TrimForExport(0, 2);
            Assert.AreEqual(2, span.Length);
            Assert.AreEqual(3, span[1].Value);
            Assert.AreEqual(ColumnValueState.Calculated, span[1].State);
            testClass.OutputToBuffer();
            testClass.Reset();
            Assert.AreEqual(ColumnValueState.Uncalculated, span[1].State);

        }
    }
}
