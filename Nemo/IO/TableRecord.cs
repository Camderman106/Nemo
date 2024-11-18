using System.Collections;
using System.Diagnostics.CodeAnalysis;
namespace Nemo.IO;

public record TableRecord : TableRow, IReadOnlyDictionary<string, string>
{
    private IReadOnlyDictionary<string, int> HeaderIndex;
    public TableRecord(int index, string[] values, IReadOnlyDictionary<string, int> headerIndex) : base(index, values)
    {
        HeaderIndex = headerIndex;
    }

    public string this[string key] => Values[HeaderIndex[key]];

    public IEnumerable<string> Keys => HeaderIndex.Keys;

    public int Count => HeaderIndex.Count;

    IEnumerable<string> IReadOnlyDictionary<string, string>.Values => HeaderIndex.Values.Select(x => Values[x]);

    public bool ContainsKey(string key)
    {
        return HeaderIndex.ContainsKey(key);
    }

    public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
    {
        return HeaderIndex.Keys.Select(x => new KeyValuePair<string, string>(x, this[x])).GetEnumerator();
    }

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
    {
        if (HeaderIndex.TryGetValue(key, out int index))
        {
            value = Values[index];
            return true;
        }
        value = null;
        return false;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return ((IEnumerable)HeaderIndex).GetEnumerator();
    }
}
