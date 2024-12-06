using Nemo.IO;
using Nemo.IO.CSV;
using Nemo.Model;
using Nemo.Model.Components;
namespace Nemo.Tests
{
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
        public SimpleExampleModel(ModelContext context) : base(context)
        {
            YieldCurve = Table.From(Context.Sources.Tables["YieldCurve"]).IndexByColumns(["T"]);

            DOB = new Scalar<string>("DOB");
            AgeAtEntry = new Scalar<double>("AgeAtEntry");
            SumAssured = new Scalar<double>("SumAssured");
            PolTermY = new Scalar<int>("PolTermY");
            Age = new Column(this, "Age", context, AggregationMethod.Average, (t) =>
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

            TPX = new Column(this, "TPX", context, AggregationMethod.Average, (t) =>
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

            CummulativeTPX = new Column(this, "CummulativeTPX", context, AggregationMethod.Average, (t) =>
            {
                if (t == 0) return 1.0;
                return CummulativeTPX.At(t - 1) * TPX.At(t);
            });

            DiscountFactors = new Column(this, "Disc_Fac", context, AggregationMethod.Average, (t) =>
            {
                if (t < 12) return 1.0;
                return Math.Pow(1 + YieldCurve.LookupDouble([(t / 12).ToString()], "Curve", () => YieldCurve.LookupDouble(["40"], "Curve")) / 100, 1d / 12) - 1;
            });

            CumulativeDiscountFactors = new Column(this, "Cum_Disc_Fac", context, AggregationMethod.Average, (t) =>
            {
                if (t == 0) return 1;
                return CumulativeDiscountFactors.At(t - 1) * (1 + DiscountFactors.At(t));
            });

            Reserve = new Column(this, "Reserve", context, AggregationMethod.Sum, (t) =>
            {
                return SumAssured * 1 / CumulativeDiscountFactors.At(t) * CummulativeTPX.At(t);
            });

            MapDataToScalar<string>("DOB", DOB, Convert.ToString);
            MapDataToScalar<double>("AGE_AT_ENTRY", AgeAtEntry, Convert.ToDouble);
            MapDataToScalar<double>("SUM_ASSURED", SumAssured, Convert.ToDouble);
            MapDataToScalar<int>("POL_TERM_Y", PolTermY, Convert.ToInt32);
        }
    }

    [TestClass]
    public sealed class ETESimpleModelTest
    {
        [TestMethod]
        public void TestEndToEnd()
        {
            var sources = new SourceManager()
                .AddCSVSource("YieldCurve", "2023Q3YieldCurve.csv")
                .SetDataSource("FakeDataWithGroup.csv");
            Projection projection = new Projection(0, 0, 1200, 1200);
            OutputSet outputSet = new OutputSet();
            ModelContext job = new ModelContext("test", Directory.GetCurrentDirectory(),  projection, outputSet, sources);

            Engine<SimpleExampleModel> engine = new Engine<SimpleExampleModel>((job) => new SimpleExampleModel(job));
            engine.Execute(job);

            

        }
        [TestMethod]
        public void Test2EndToEnd()
        {
            var sources = new SourceManager()
                .AddCSVSource("YieldCurve", "2023Q3YieldCurve.csv")
                .SetDataSource("FakeDataWithGroup.csv");
            Projection projection = new Projection(0, 0, 1200, 1200);
            OutputSet outputSet = new OutputSet();

            ModelContext job2 = new ModelContext("test2", Directory.GetCurrentDirectory(), projection, outputSet, sources);
            var engine = new Engine<SimpleExampleModel>((job2) => new SimpleExampleModel(job2));
            engine.GroupRecordsBy("GROUP");
            engine.Execute(job2);
        }
        [TestMethod]
        public void Test3EndToEnd()
        {
            var sources = new SourceManager()
                .AddCSVSource("YieldCurve", "2023Q3YieldCurve.csv")
                .SetDataSource("FakeDataWithGroup.csv");
            Projection projection = new Projection(0, 0, 1200, 1200);
            OutputSet outputSet = new OutputSet();

            ModelContext job3 = new ModelContext("test3", Directory.GetCurrentDirectory(), projection, outputSet, sources);
            var engine = new Engine<SimpleExampleModel>((job2) => new SimpleExampleModel(job3));
            engine.GroupRecordsBy("GROUP");
            engine.Execute(job3);
        }
    }
}
