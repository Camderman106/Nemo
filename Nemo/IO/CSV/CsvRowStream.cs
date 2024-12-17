using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Nemo.IO.CSV;

public class CsvRowStream : IDisposable
{
    private readonly Stream stream;
    private readonly byte[] bytes;
    private char[] chars;
    private int charPos = 0;
    private int charLen = 0;
    private long byteChunkBeginPosition = 0;
    private long byteChunkEndPosition = 0;
    private readonly Encoding encoding;
    private readonly Decoder decoder;
    private const int InitialBufferSize = 1024;
    private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);
    //public long EndLineByteOffsetInclusive { get; private set; } = 0;

    public CsvRowStream(CSVSource source)
    {
        this.stream = new FileStream(source.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        bytes = ArrayPool<byte>.Shared.Rent(InitialBufferSize);
        chars = ArrayPool<char>.Shared.Rent(InitialBufferSize);
        encoding = DetectEncoding(stream, DefaultEncoding);
        //EndLineByteOffsetInclusive = -1;
        decoder = encoding.GetDecoder();
        BytePosOfNextLine = stream.Position;
        BytePosOfCurrLine = BytePosOfNextLine;
        byteChunkBeginPosition = stream.Position;
        byteChunkEndPosition = stream.Position;
    }

    public CsvRowStream(Stream stream)
    {
        this.stream = stream;
        bytes = ArrayPool<byte>.Shared.Rent(InitialBufferSize);
        chars = ArrayPool<char>.Shared.Rent(InitialBufferSize);
        encoding = DetectEncoding(stream, DefaultEncoding);
        //EndLineByteOffsetInclusive = -1;
        decoder = encoding.GetDecoder();
        BytePosOfNextLine = stream.Position;
        BytePosOfCurrLine = BytePosOfNextLine;
        byteChunkBeginPosition = stream.Position;
        byteChunkEndPosition = stream.Position;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]

    public void Seek(long byteOffset)
    {
        byteOffset = Math.Max(byteOffset, encoding.Preamble.Length);
        // Validate the byteOffset
        if (byteOffset < 0 || byteOffset > stream.Length)
            throw new ArgumentOutOfRangeException(nameof(byteOffset));
        BytePosOfNextLine = byteOffset;
        BytePosOfCurrLine = byteOffset;
        if (byteOffset >= byteChunkBeginPosition && byteOffset < byteChunkEndPosition)
        {
            // The desired position is within the current byte buffer
            // Adjust positions and re-decode if necessary
            int byteOffsetInBuffer = (int)(byteOffset - byteChunkBeginPosition);

            // Reset decoder
            decoder.Reset();

            // Re-decode bytes from byteOffsetInBuffer
            charPos = 0;
            charLen = decoder.GetChars(bytes, byteOffsetInBuffer, (int)(byteChunkEndPosition - byteOffset), chars, 0);
        }
        else
        {
            // The desired position is outside the current buffer
            // Seek the stream and reset buffers
            stream.Seek(byteOffset, SeekOrigin.Begin);
            byteChunkBeginPosition = byteOffset;
            byteChunkEndPosition = byteOffset;


            // Reset decoder and buffers
            decoder.Reset();
            charPos = 0;
            charLen = 0;

            // Load new data
            LoadNextChunk();
        }
    }

    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]

    private static int FindNewLine(char[] buffer, int startPos, int endPos)
    {
        for (int i = startPos; i < endPos; i++)
        {
            if (buffer[i] == '\n' || buffer[i] == '\r')
            {
                return i;
            }
        }
        return -1;
    }
    public long BytePosOfCurrLine { get; private set; }
    public long BytePosOfNextLine { get; private set; }
    //public IEnumerable<ReadOnlySpan<char>> EnumerateFromPosition()
    //{
    //    while (!EOFFlag)
    //    {
    //        yield return GetLine();
    //    }
    //}
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public bool GetLine(out ReadOnlySpan<char> result)
    {
        BytePosOfCurrLine = BytePosOfNextLine;
        
        while (true)
        {
            // Search for newline characters in chars[] from charPos to charLen
            int newLinePos = FindNewLine(chars, charPos, charLen);

            if (newLinePos >= 0)
            {
                // Newline found
                int lineStart = charPos;
                int lineLength = newLinePos - lineStart;
                ReadOnlySpan<char> line = new ReadOnlySpan<char>(chars, lineStart, lineLength);
                
                // Advance charPos past the newline character(s)
                charPos = newLinePos + 1;

                // Handle '\r\n' by checking if the next character is '\n'
                if (chars[newLinePos] == '\r' && charPos < charLen && chars[charPos] == '\n')
                {
                    charPos++;
                }
                //If this is in a power of 2 position, and the next character is /0 then the /n might be the first byte in the next chunk. Handle this case in the LoadNextChunk function later

                BytePosOfNextLine = BytePosOfCurrLine + encoding.GetByteCount(new ReadOnlySpan<char>(chars, lineStart, charPos - lineStart));
                result = line;
                return true;
            }
            if (LoadNextChunk() == 0)
            {
                // End of file
                if (charPos < charLen)
                {
                    // Return remaining data as the last line
                    ReadOnlySpan<char> line = new ReadOnlySpan<char>(chars, charPos, charLen - charPos);
                    BytePosOfNextLine = BytePosOfCurrLine + encoding.GetByteCount(line);
                    charPos = charLen;
                    result = line;
                    return false;
                }
                else
                {
                    // No more data
                    result = ReadOnlySpan<char>.Empty;
                    return false;
                }
            }
        }
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private int LoadNextChunk()
    {
        // Shift the buffer if necessary
        if (charPos > 0)
        {
            int remainingChars = charLen - charPos;
            Array.Copy(chars, charPos, chars, 0, remainingChars);
            charLen = remainingChars;
            charPos = 0;
        }

        // Read bytes from the stream
        int bytesRead = stream.Read(bytes, 0, bytes.Length);
        byteChunkBeginPosition = stream.Position - bytesRead;
        byteChunkEndPosition = stream.Position;

        if (bytesRead == 0)
        {
            // End of file
            return 0;
        }

        // Decode bytes into chars[]
        int maxCharsNeeded = encoding.GetMaxCharCount(bytesRead);

        // Ensure chars[] has enough space
        if (charLen + maxCharsNeeded > chars.Length)
        {
            ExpandCharBuffer(charLen + maxCharsNeeded);
        }

        int charsDecoded = decoder.GetChars(bytes, 0, bytesRead, chars, charLen);
        charLen += charsDecoded;

        //This handles the possibility that either the first one or two characters from the new chunk are the newline characters
        //We just skip up to twice and adjust the byte position variable appropriately
        //Note that the current position is only set once at the start of any Getline instance so is effectively constant. We only need to ensure the Next position is correct
        if (chars[charPos] == '\r' || chars[charPos] == '\n')
        {
            charPos++;
            BytePosOfNextLine += encoding.GetByteCount(chars, charPos, 1);
        }
        //I actually think we only ever need one of these as if both characters are in the next chunk then then line wont have been cut out yet. 
        //if (chars[charPos] == '\r' || chars[charPos] == '\n')
        //{
        //    charPos++;
        //    BytePosOfNextLine += encoding.GetByteCount(chars, charPos, 1);
        //}

        return charsDecoded;
    }

    private void ExpandCharBuffer(int newSize)
    {
        var newChars = ArrayPool<char>.Shared.Rent(newSize);
        Array.Copy(chars, 0, newChars, 0, charLen);
        ArrayPool<char>.Shared.Return(chars);
        chars = newChars;
    }

    // Known BOMs and their corresponding encodings
    private static readonly Dictionary<byte[], Encoding> BomEncodings = new()
    {
        { new byte[] { 0xEF, 0xBB, 0xBF }, Encoding.UTF8 },
        { new byte[] { 0xFF, 0xFE }, Encoding.Unicode }, // UTF-16 LE
        { new byte[] { 0xFE, 0xFF }, Encoding.BigEndianUnicode }, // UTF-16 BE
        { new byte[] { 0xFF, 0xFE, 0x00, 0x00 }, Encoding.UTF32 }, // UTF-32 LE
        { new byte[] { 0x00, 0x00, 0xFE, 0xFF }, Encoding.GetEncoding("utf-32BE") } // UTF-32 BE
    };        
    private static Encoding DetectEncoding(Stream stream, Encoding defaultEncoding)
    {
        long originalPosition = stream.Position;

        // Read enough bytes to check for a BOM
        byte[] buffer = new byte[4];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);

        // Try to match BOMs
        foreach (var kvp in BomEncodings)
        {
            byte[] bom = kvp.Key;
            if (bytesRead >= bom.Length && MatchesBom(buffer, bom))
            {
                // Adjust stream position to skip BOM
                stream.Position = originalPosition + bom.Length;                
                return kvp.Value;
            }
        }

        // No BOM found, reset position and return default encoding
        stream.Position = originalPosition;
        return defaultEncoding;
    }
    private static bool MatchesBom(byte[] buffer, byte[] bom)
    {
        for (int i = 0; i < bom.Length; i++)
        {
            if (buffer[i] != bom[i])
                return false;
        }
        return true;
    }
    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(bytes, true);
        ArrayPool<char>.Shared.Return(chars, true);
    }

}

