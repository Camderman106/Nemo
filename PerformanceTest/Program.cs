using Nemo.Tests;

namespace PerfTest;

public class Program
{
    public static void Main()
    {
        IOTests iOTests = new IOTests();
        iOTests.TestLookupsPerformance();
    }
}