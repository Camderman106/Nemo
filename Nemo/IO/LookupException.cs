namespace Nemo.IO;

internal class LookupException : Exception
{
    public LookupException()
    {
    }

    public LookupException(string? message) : base(message)
    {
    }

    public LookupException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
