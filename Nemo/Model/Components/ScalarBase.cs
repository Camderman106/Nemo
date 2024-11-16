using System.Diagnostics;

namespace Nemo.Model.Components
{
    [DebuggerDisplay("{Name}: {typeof(T)} {_value}")]
    public class ScalarBase
    {
        public string OutputName { get; init; }
        internal bool IsOutput { get; set; }
    }
}