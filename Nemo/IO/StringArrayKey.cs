namespace Nemo.IO;

public class StringArrayKey : IEquatable<StringArrayKey>
{
    internal string[] Strings { get; init; }
    private readonly int hashcode;

    public StringArrayKey(string[] strings)
    {
        Strings = strings ?? throw new ArgumentNullException(nameof(strings));
        hashcode = CalulateHashCode(strings);
    }
    public StringArrayKey(ReadOnlySpan<string> strings)
    {
        Strings = strings.ToArray() ?? throw new ArgumentNullException(nameof(strings));
        hashcode = CalulateHashCode(strings);
    }

    public bool Equals(StringArrayKey? other)
    {
        if (other is null)
            return false;

        // Check if the arrays have the same length and elements
        //return this.hashcode == other.hashcode;
        return Strings.SequenceEqual(other.Strings);
    }

    public override bool Equals(object? obj)
    {
        if (obj is StringArrayKey otherWrapper)
            return Equals(otherWrapper);
        return false;
    }

    public override int GetHashCode()
    {
        return hashcode;
        // Generate a hash code based on the elements in the array
        //return Strings.Aggregate(0, (hash, str) => hash ^ (str?.GetHashCode() ?? 0));
    }
    public int CalulateHashCode(ReadOnlySpan<string> span)
    {        
        unchecked
        {
            int hash = 17;
            foreach (var str in span)
            {
                hash = hash * 31 + (str != null ? str.GetHashCode() : 0);
            }
            return hash;
        }
    }

    public override string ToString()
    {
        return $"[{string.Join(", ", Strings)}]";
    }
}
