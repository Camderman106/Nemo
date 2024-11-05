namespace Nemo.Model;

public class Projection
{
    public Projection(int min, int start, int end, int max)
    {
        T_Min = min;
        T_Start = start;
        T_End = end;
        T_Max = max;
    }

    public int T_Min { get; init; }
    public int T_Start { get; init; }
    public int T_End { get; init; }
    public int T_Max { get; init; }

}
