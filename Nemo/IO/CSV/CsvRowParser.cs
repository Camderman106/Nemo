namespace Nemo.IO.CSV;

using System;
using System.Buffers;
using System.Diagnostics;

public class CsvRowParser : IDisposable
{
    private enum ParseState
    {
        FIELD_START,
        IN_FIELD_UNQUOTED,
        IN_FIELD_QUOTED,
        END_OF_QUOTED_FIELD,
    }

    private struct FieldPosition
    {
        public int Start;
        public int Length;
    }

    private FieldPosition[] fieldPositions;
    internal char[] chars;
    private readonly char SEPARATOR;
    private const char QUOTE = '"';
    private int expectedFieldCount = -1;
    public int FieldCount { get; private set; }
    public bool TrimWhitespace { get; }

    public CsvRowParser(char separator = ',', bool validateRows = false, bool trimWhitespace = false)
    {
        fieldPositions = ArrayPool<FieldPosition>.Shared.Rent(16);
        chars = ArrayPool<char>.Shared.Rent(64);
        expectedFieldCount = validateRows ? -1 : -2;
        SEPARATOR = separator;
        TrimWhitespace = trimWhitespace;
    }

    public void Parse(ReadOnlySpan<char> inputRow)
    {
        // Return previous arrays to the pool
        ArrayPool<FieldPosition>.Shared.Return(fieldPositions, clearArray: true);
        ArrayPool<char>.Shared.Return(chars, clearArray: false);

        // Rent new arrays based on input size
        int charsLength = inputRow.Length > 0 ? inputRow.Length : 64;
        chars = ArrayPool<char>.Shared.Rent(charsLength);
        fieldPositions = ArrayPool<FieldPosition>.Shared.Rent(inputRow.Length + 1); // +1 for potential empty field
        FieldCount = 0;

        int readPos = 0;
        int writePos = 0;
        int fieldStart = 0;

        ParseState state = ParseState.FIELD_START;

        while (readPos < inputRow.Length)
        {
            char c = inputRow[readPos];
            char next = (readPos + 1 < inputRow.Length) ? inputRow[readPos + 1] : '\0';
            if (c == '\r' || c == '\n') { throw new FormatException("This parser only handles single rows. Line breaks cannot exist"); }
            switch (state)
            {
                case ParseState.FIELD_START:
                    if (TrimWhitespace)
                    {
                        // Skip leading whitespace outside quotes
                        while (readPos < inputRow.Length 
                            && char.IsWhiteSpace(c) 
                            && c != SEPARATOR)
                        {
                            readPos++;
                            if (readPos < inputRow.Length)
                                c = inputRow[readPos];
                            else
                                break;
                        }
                    }

                    if (c == SEPARATOR)
                    {
                        // Empty field
                        RecordField(fieldStart, 0);
                        readPos++;
                        fieldStart = writePos;
                    }
                    else if (c == QUOTE)
                    {
                        if(next != QUOTE)
                        {
                            state = ParseState.IN_FIELD_QUOTED;
                            readPos++;
                        }
                        else
                        {
                            state = ParseState.IN_FIELD_UNQUOTED;
                            chars[writePos++] = c;
                            readPos+=2;
                        }
                        
                    }                                        
                    else
                    {
                        state = ParseState.IN_FIELD_UNQUOTED;
                        chars[writePos++] = c;
                        readPos++;
                    }
                    break;

                case ParseState.IN_FIELD_UNQUOTED:
                    if (c == SEPARATOR)
                    {
                        // End of unquoted field
                        int fieldLength = writePos - fieldStart;
                        if (TrimWhitespace)
                        {
                            // Trim trailing whitespace
                            while (fieldLength > 0 && char.IsWhiteSpace(chars[fieldStart + fieldLength - 1]))
                            {
                                Debug.Assert(chars[fieldStart + fieldLength - 1] != (char)0);
                                fieldLength--; 
                            }
                        }
                        RecordField(fieldStart, fieldLength);
                        readPos++;
                        fieldStart = writePos;
                        state = ParseState.FIELD_START;
                    }
                    else if (c == QUOTE)
                    {                        
                        // Escaped quote
                        chars[writePos++] = QUOTE;
                        readPos++;                        
                    }                    
                    else
                    {
                        // Regular character
                        chars[writePos++] = c;
                        readPos++;
                    }
                    break;

                case ParseState.IN_FIELD_QUOTED:
                    if (c == QUOTE)
                    {
                        if (next == QUOTE)
                        {
                            // Escaped quote
                            chars[writePos++] = QUOTE;
                            readPos += 2;
                        }
                        else
                        {
                            // End of quoted field
                            state = ParseState.END_OF_QUOTED_FIELD;
                            readPos++;
                        }
                    }
                    else
                    {
                        // Inside quoted field
                        chars[writePos++] = c;
                        readPos++;
                    }
                    break;

                case ParseState.END_OF_QUOTED_FIELD:
                    if (c == SEPARATOR)
                    {
                        // End of quoted field
                        RecordField(fieldStart, writePos - fieldStart);
                        readPos++;
                        fieldStart = writePos;
                        state = ParseState.FIELD_START;
                    }
                    else
                    {
                        if(TrimWhitespace && char.IsWhiteSpace(c))
                        {
                            readPos++;
                        }
                        // Invalid character after closing quote
                        else
                        {
                            throw new FormatException($"Invalid character '{c}' after closing quote at position {readPos}");
                        }
                    }
                    break;

                
                default:
                    throw new InvalidOperationException($"Unknown parse state: {state}");
            }
        }

        // Handle end of input
        switch (state)
        {
            case ParseState.FIELD_START:
                RecordField(writePos, 0);                
                break;

            case ParseState.IN_FIELD_UNQUOTED:
                int finalFieldLength = writePos - fieldStart;
                if (TrimWhitespace)
                {
                    // Trim trailing whitespace
                    while (finalFieldLength > 0 && char.IsWhiteSpace(chars[fieldStart + finalFieldLength - 1]))
                        finalFieldLength--;
                }
                RecordField(fieldStart, finalFieldLength);
                break;

            case ParseState.IN_FIELD_QUOTED:
                throw new FormatException("Unexpected end of input inside a quoted field");

            case ParseState.END_OF_QUOTED_FIELD:
                RecordField(fieldStart, writePos - fieldStart);
                break;
                            
            default:
                throw new InvalidOperationException($"Unknown parse state at end of input: {state}");
        }

        // Handle field count validation
        if (expectedFieldCount == -1)
        {
            // First row, set the expected field count
            expectedFieldCount = FieldCount;
        }
        else if (expectedFieldCount >= 0 && FieldCount != expectedFieldCount)
        {
            throw new FormatException($"Row has different number of fields. Expected: {expectedFieldCount}, Actual: {FieldCount}");
        }
    }

    private void RecordField(int start, int length)
    {
        EnsureFieldCapacity();
        fieldPositions[FieldCount++] = new FieldPosition { Start = start, Length = length };
    }

    private void EnsureFieldCapacity()
    {
        if (FieldCount >= fieldPositions.Length)
        {
            // Double the size or set to a minimum size
            int newSize = fieldPositions.Length == 0 ? 16 : fieldPositions.Length * 2;
            var newArray = ArrayPool<FieldPosition>.Shared.Rent(newSize);
            Array.Copy(fieldPositions, newArray, fieldPositions.Length);
            ArrayPool<FieldPosition>.Shared.Return(fieldPositions, clearArray: true);
            fieldPositions = newArray;
        }
    }

    public ReadOnlySpan<char> GetField(int index)
    {
        if (index < 0 || index >= FieldCount)
        {
            throw new IndexOutOfRangeException($"Field {index} not found");
        }

        var fieldPos = fieldPositions[index];
        return chars.AsSpan(fieldPos.Start, fieldPos.Length);
    }

    public void Dispose()
    {
        ArrayPool<FieldPosition>.Shared.Return(fieldPositions, clearArray: true);
        ArrayPool<char>.Shared.Return(chars, clearArray: false);
    }
}

