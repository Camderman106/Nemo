namespace Nemo.IO;

public class StringArrayKey : IEquatable<StringArrayKey>
{
    internal string[] Strings { get; init; }

    public StringArrayKey(string[] strings)
    {
        Strings = strings ?? throw new ArgumentNullException(nameof(strings));
    }
    public StringArrayKey(ReadOnlySpan<string> strings)
    {
        Strings = strings.ToArray() ?? throw new ArgumentNullException(nameof(strings));
    }

    public bool Equals(StringArrayKey? other)
    {
        if (other is null)
            return false;

        // Check if the arrays have the same length and elements
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
        // Generate a hash code based on the elements in the array
        return Strings.Aggregate(0, (hash, str) => hash ^ (str?.GetHashCode() ?? 0));
    }

    public override string ToString()
    {
        return $"[{string.Join(", ", Strings)}]";
    }
}
