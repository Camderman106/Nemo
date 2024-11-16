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
            Assert.AreEqual(3, testClass.column1.View(1));
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

    [TestClass]
    public sealed class TestSimpleModel
    {
        [TestMethod]
        public void TestEndToEnd()
        {
            CSVSource yieldcurve = new CSVSource("2023Q3YieldCurve.csv");
            CSVSource data = new CSVSource("FakeDataWithGroup.csv");
            var sources = new Dictionary<string, CSVSource>();
            sources["YieldCurve"] = yieldcurve;
            Projection projection = new Projection(0, 0, 1200, 1200, sources);
            OutputSet outputSet = new OutputSet();
            Job job = new Job("test", Directory.GetCurrentDirectory(), data, projection, outputSet);

            Engine<SimpleExampleModel> engine = new Engine<SimpleExampleModel>((projection, outputSet) => new SimpleExampleModel(projection, outputSet));
            engine.Execute(job);

            Job job2 = new Job("test2", Directory.GetCurrentDirectory(), data, projection, outputSet);
            engine = new Engine<SimpleExampleModel>((projection, outputSet) => new SimpleExampleModel(projection, outputSet));
            engine.GroupRecordsBy("GROUP");
            engine.Execute(job2);

        }
    }
    public sealed class SimpleExampleModel : ModelBase
    {
        Scalar<string> DOB;
        Scalar<double> AgeAtEntry;
        Scalar<double> SumAssured;
        Scalar<int> PolTermY;

        Scalar<string> VALDATE = new Scalar<string>("VALDATE", () => "30/09/2024");

        Column Age;
        Column TPX;
        Column CummulativeTPX;
        Column DiscountFactors;
        Column CumulativeDiscountFactors;

        Column Reserve;

        Table.ColumnIndexed YieldCurve;
        public SimpleExampleModel(Projection projection, OutputSet outputSet) : base(projection, outputSet)
        {
            YieldCurve = Table.From(projection.Tables["YieldCurve"]).IndexByColumns(["T"]);

            DOB = new Scalar<string>("DOB");
            AgeAtEntry = new Scalar<double>("AgeAtEntry");
            SumAssured = new Scalar<double>("SumAssured");
            PolTermY = new Scalar<int>("PolTermY");

            Age = new Column("Age", projection.T_Min, projection.T_Max, AggregationMethod.Average, (t) =>
            {
                DateOnly birthDate = DateOnly.Parse(DOB);
                DateOnly valuationDate = DateOnly.Parse(VALDATE).AddMonths(t);
                int years = valuationDate.Year - birthDate.Year;
                DateOnly lastBirthday = birthDate.AddYears(years);

                if (valuationDate < lastBirthday)
                {
                    years--;
                    lastBirthday = birthDate.AddYears(years);
                }

                double daysSinceLastBirthday = (valuationDate.ToDateTime(TimeOnly.MinValue) - lastBirthday.ToDateTime(TimeOnly.MinValue)).TotalDays;
                double daysInYear = DateTime.IsLeapYear(valuationDate.Year) ? 366 : 365;

                return years + (daysSinceLastBirthday / daysInYear);

            }
            );

            TPX = new Column("TPX", projection.T_Min, projection.T_Max, AggregationMethod.Average, (t) =>
            {
                // Parameters for the survival function
                const double maxAge = 120.0; // Maximum age, 100% mortality
                const double beta = 10.0;    // Controls the steepness of the survival curve
                const double alpha = 0.04;   // Base mortality increase per year

                // Ensure age does not exceed maxAge
                if (Age.At(t) >= maxAge) return 0.0;

                // Survival probability formula
                static double survivalProbability(double x) =>
                    Math.Max(0.0, (maxAge - x) / maxAge) * Math.Exp(-alpha * x);

                // Calculate tpx = probability of surviving t years starting at age
                double pxCurrent = survivalProbability(Age.At(t));
                double pxFuture = survivalProbability(Age.At(t + 1));

                return Math.Max(0.0, pxFuture / pxCurrent);
            });

            CummulativeTPX = new Column("CummulativeTPX", projection.T_Min, projection.T_Max, AggregationMethod.Average, (t) =>
            {
                if (t==0) return 1.0;
                return CummulativeTPX.At(t-1) * TPX.At(t);
            });

            DiscountFactors = new Column("Disc_Fac", projection.T_Min, projection.T_Max, AggregationMethod.Average, (t) =>
            {
                if (t<12) return 1.0;
                return Math.Pow(1 + YieldCurve.LookupDouble([(t / 12).ToString()], "Curve", () =>  YieldCurve.LookupDouble(["40"], "Curve"))/100, 1d/12) - 1;
            });

            CumulativeDiscountFactors = new Column("Cum_Disc_Fac", projection.T_Min, projection.T_Max, AggregationMethod.Average, (t) =>
            {
                if (t == 0) return 1;
                return CumulativeDiscountFactors.At(t-1) * (1 + DiscountFactors.At(t));
            });

            Reserve = new Column("Reserve", projection.T_Min, projection.T_Max, AggregationMethod.Sum, (t) =>
            {
                return SumAssured * 1/CumulativeDiscountFactors.At(t) * CummulativeTPX.At(t);
            });

            MapDataToScalar<string>("DOB", DOB, Convert.ToString);
            MapDataToScalar<double>("AGE_AT_ENTRY", AgeAtEntry, Convert.ToDouble);
            MapDataToScalar<double>("SUM_ASSURED", SumAssured, Convert.ToDouble);
            MapDataToScalar<int>("POL_TERM_Y", PolTermY, Convert.ToInt32);
        }
    }
}
