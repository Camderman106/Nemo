﻿using Nemo;
using Nemo.IO;
using Nemo.Model;
using Nemo.Model.Components;
using Table = Nemo.IO.CSV.Table;

namespace ExampleModel;
public class ExampleModel : ModelBase
{
    public static void Main()
    {
        //var job = new ModelContext(
        //    "Example10k",
        //    Directory.GetCurrentDirectory(),
        //    new Projection(0, 0, 1200, 1200),
        //    OutputSet.Default(),
        //    new SourceManager()
        //    .SetDataSource("Inputs\\ExampleData10k.csv")
        //    .AddCSVSource("DiscountCurve", "Inputs\\SpotRates.csv")
        //    .AddExcelSource("AFC00", "Inputs\\AFC00.xlsx", "AFC00", "A3:D108")
        //    .AddExcelSource("AMC00", "Inputs\\AMC00.xlsx", "AMC00", "A3:D108")
        //    );
        var job = new ModelContext(
            "Example100k",
            Directory.GetCurrentDirectory(),
            new Projection(0, 0, 1200, 1200),
            OutputSet.Default(),
            new SourceManager()
            .SetDataSource("Inputs\\ExampleData100k.csv")
            .AddCSVSource("DiscountCurve", "Inputs\\SpotRates.csv")
            .AddExcelSource("AFC00", "Inputs\\AFC00.xlsx", "AFC00", "A3:D108")
            .AddExcelSource("AMC00", "Inputs\\AMC00.xlsx", "AMC00", "A3:D108")
            );
        //var job = new ModelContext(
        //    "ExampleReference",
        //    Directory.GetCurrentDirectory(),
        //    new Projection(0, 0, 120, 120),
        //    OutputSet.Default(),
        //    new SourceManager()
        //    .SetDataSource("Inputs\\ExampleDataTraceTest.csv")
        //    .AddCSVSource("DiscountCurve", "Inputs\\SpotRates.csv")
        //    .AddExcelSource("AFC00", "Inputs\\AFC00.xlsx", "AFC00", "A3:D108")
        //    .AddExcelSource("AMC00", "Inputs\\AMC00.xlsx", "AMC00", "A3:D108")
        //    );
        //var job = new ModelContext(
        //    "ExampleTrace",
        //    Directory.GetCurrentDirectory(),
        //    new Projection(0, 0, 120, 120),
        //    OutputSet.Default(),
        //    new SourceManager()
        //    .SetDataSource("Inputs\\ExampleDataTraceTest.csv")
        //    .AddCSVSource("DiscountCurve", "Inputs\\SpotRates.csv")
        //    .AddExcelSource("AFC00", "Inputs\\AFC00.xlsx", "AFC00", "A3:D108")
        //    .AddExcelSource("AMC00", "Inputs\\AMC00.xlsx", "AMC00", "A3:D108")
        //    )
        //    .SetTraceTable("ExampleReference-ExampleModel.csv");

        var engine = new Engine<ExampleModel>((x) => new ExampleModel(x))
            .GroupRecordsBy("GROUP")
            .UseMultiThreading(true)
            .UseChunkSize(500)
            ;
        engine.Execute(job);
    }

    Scalar<string> pol_type = new Scalar<string>("POL_TYPE");
    Scalar<double> sum_assured = new Scalar<double>("SUM_ASSURED");
    Scalar<int> age_initial = new Scalar<int>("AGE");
    Scalar<int> sex = new Scalar<int>("SEX");

    Column qx;
    Column age;
    Column tpx;
    Column discount_rate;
    Column discount_factor;
    Column reserve_per;
    Column reserve;

    //SharedColumn AMC00_qx;
    //SharedColumn AFC00_qx;
    //SharedColumn discount_shared;

    Table.ColumnIndexed AMC00;
    Table.ColumnIndexed AFC00;
    Table.ColumnIndexed Spots;
    public ExampleModel(ModelContext context) : base(context)
    {
        AMC00 = Table.From(context.Sources.Tables["AMC00"]).IndexByColumns(["Age x"]);
        AFC00 = Table.From(context.Sources.Tables["AFC00"]).IndexByColumns(["Age x"]);
        Spots = Table.From(context.Sources.Tables["DiscountCurve"]).IndexByColumns(["Time"]);

        age = new Column(this,
            "age", 
            context,
            AggregationMethod.Average,
            (t) => age_initial + t
        );

        //AMC00_qx = new SharedColumn("shared_qx_m", context, (t) => AMC00.LookupDouble([(t).ToString()], "Durations 2+", () => 1));
        //AFC00_qx = new SharedColumn("shared_qx_f", context, (t) => AFC00.LookupDouble([(t).ToString()], "Durations 2+", () => 1));

        qx = new Column(this,
            "qx",
            context,
            AggregationMethod.Average,
            (t) =>
            {
                if(sex == 0)
                {
                    //return AMC00_qx.At(Math.Min((int)age.At(t), context.Projection.T_Max));
                    return AMC00.LookupDouble([((int)age.At(t)).ToString()], "Durations 2+", () => 1);
                }
                else
                {
                    //return AFC00_qx.At(Math.Min((int)age.At(t), context.Projection.T_Max));
                    return AFC00.LookupDouble([((int)age.At(t)).ToString()], "Durations 2+", () => 1);

                }
            }
        );

        tpx = new Column(this,
            "tpx",
            context,
            AggregationMethod.Average,
            (t) =>
            {
                if (t == 0) return 1;
                return tpx!.At(t-1) * (1-qx.At(t));
            }
        );

        //discount_shared = new SharedColumn("shared_discount", context, (t) =>
        //{
        //    if (t == 0) return 1;
        //    return Spots.LookupDouble([((int)t).ToString()], "Spot",
        //        () => Spots.LookupDouble([((int)40).ToString()], "Spot")) / 100;
        //});
        discount_rate = new Column(this,
            "discount_rate",
            context,
            AggregationMethod.Average,
            (t) =>
            {
                //return discount_shared.At(t);
                if (t == 0) return 1;
                return Spots.LookupDouble([((int)t).ToString()], "Spot",  () => Spots.LookupDouble([((int)40).ToString()], "Spot")) /100;
            }
        );

        discount_factor = new Column(this,
            "discount_factor",
            context,
            AggregationMethod.Average,
            (t) =>
            {
                if (t == 0) return 1;
                return discount_factor!.At(t-1)* (1/(1+discount_rate.At(t)));
            }
        );

        reserve_per = new Column(this,
            "reserve_per",
            context,
            AggregationMethod.Sum,
            (t) =>
            {
                if (t == 0) return qx.At(t) * discount_factor.At(t) * sum_assured;
                else return qx.At(t) * discount_factor.At(t) * tpx.At(t-1) * sum_assured;
            }
        );

        reserve = new Column(this,
            "reserve",
            context,
            AggregationMethod.Sum,
            (t) =>
            {
                if (t == context.Projection.T_Max) return reserve_per.At(t);
                return reserve_per.At(t) + reserve!.At(t + 1);
            });

        MapDataToScalar<string>("PRODUCT", pol_type!, Convert.ToString);
        MapDataToScalar<int>("AGE", age_initial!, Convert.ToInt32);
        MapDataToScalar<double>("SUM_ASSURED", sum_assured!, Convert.ToDouble);
        MapDataToScalar<int>("SEX", sex!, (s) => s == "F" ? 1 : 0);
    }
}