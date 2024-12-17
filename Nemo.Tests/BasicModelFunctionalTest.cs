using Nemo.IO;
using Nemo.Model;
using Nemo.Model.Components;
namespace Nemo.Tests
{
    [TestClass]
    public sealed class BasicModelFunctionalTest
    {
        class TestClass : ModelBase
        {
            internal Column column1;
            internal Column column2;
            public TestClass(ModelContext context) : base(context)
            {
                column1 = new Column(this, "TestColumn", context, AggregationMethod.Sum, (int t) => 2 * t + 1);
                column2 = new Column(this, "TestColumn2", context, AggregationMethod.Sum, (int t) => column1.At(t));
            }

        }

        [TestMethod]
        public void TestSimpleClass()
        {
            Projection proj = new Projection(0,0,1,1);
            ModelContext job = new ModelContext("", "", proj, OutputSet.Default(), new SourceManager());
            TestClass testClass = new TestClass(job);
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
