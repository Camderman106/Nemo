using Nemo.Tests;

namespace PerfTest;

public class Program
{
    public static void Main()
    {
        IOTestsCsv iOTests = new();
        iOTests.TestLookupsPerformance();
    }
}