using System.Diagnostics;

namespace Nemo.Model.Buffers;

[DebuggerDisplay("Sum({Sum})  Count({Count})")]
internal struct AggregateColumnBufferValue
{
    internal double Sum;
    internal uint Count;
}
