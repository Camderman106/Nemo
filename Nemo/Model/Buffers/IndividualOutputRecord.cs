namespace Nemo.Model.Buffers;

internal class IndividualOutputRecord
{
    internal int Index { get; init; }
    internal string Line { get; init; }
    internal IndividualOutputRecord(int index, string line)
    {
        Index = index;
        Line = line;
    }
}
