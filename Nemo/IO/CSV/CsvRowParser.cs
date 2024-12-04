namespace Nemo.IO.CSV;

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

public class CsvRowParser : IDisposable
{
    public int FieldCount { get; private set; }

    public delegate void ParserStrategy(ReadOnlySpan<char> inputRow);
    public ParserStrategy Parse;

    private FieldPosition[] fieldPositions;
    internal char[] chars;
    private readonly char SEPARATOR;
    private const char DQUOTE = '"';
    private int expectedFieldCount = -1;

    private enum ParseState
    {
        FIELD_START,
        IN_FIELD_UNQUOTED,
        IN_FIELD_QUOTED,
        FIELD_END,
    }
    public enum STRATEGY
    {
        STRICT
    }
    private void ParseStrict(ReadOnlySpan<char> inputRow)
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
            switch (state)
            {
                case ParseState.FIELD_START:
                    {
                        if (c == '\r' || c == '\n') { throw new FormatException("This parser only handles single rows. Line breaks must be escaped"); }
                        //RFC defines that we skip whitespace around quotes
                        if (char.IsWhiteSpace(c) && c != DQUOTE && c != SEPARATOR)
                        {
                            readPos++;
                            break;
                        }

                        if (c == SEPARATOR)
                        {   
                            RecordField(fieldStart, 0);
                            readPos++;
                            fieldStart = writePos;
                        }
                        else if (c == DQUOTE)
                        {
                            state = ParseState.IN_FIELD_QUOTED;
                            readPos++;
                        }
                        else
                        {
                            state = ParseState.IN_FIELD_UNQUOTED;
                            readPos++;
                            chars[writePos++] = c;
                        }
                    }
                    break;
                case ParseState.IN_FIELD_UNQUOTED:
                    {
                        if (c == '\r' || c == '\n') { throw new FormatException("This parser only handles single rows. Line breaks must be escaped"); }

                        if (c == SEPARATOR)
                        {
                            int finalFieldLength = writePos - fieldStart;
                            // Trim trailing whitespace
                            while (finalFieldLength > 0 && char.IsWhiteSpace(chars[fieldStart + finalFieldLength - 1]))
                                finalFieldLength--;
                            
                            RecordField(fieldStart, finalFieldLength);
                            readPos++;
                            fieldStart = writePos;
                            state = ParseState.FIELD_START;
                        }
                        else if (c == DQUOTE)
                        {
                            throw new FormatException($"RFC4180 dictates fields containing quotes must be enclosed in quotes. '{c}' at position {readPos}");
                        }
                        else
                        {
                            readPos++;
                            chars[writePos++] = c;
                        }
                    }
                    break;
                case ParseState.IN_FIELD_QUOTED:
                    {
                        if (c == DQUOTE)
                        {
                            if (next == DQUOTE)
                            {
                                chars[writePos++] = c;
                                readPos += 2;
                            }
                            else
                            {
                                state = ParseState.FIELD_END;
                                readPos++;
                            }
                        }
                        else
                        {
                            readPos++;
                            chars[writePos++] = c;
                        }
                    }
                    break;
                case ParseState.FIELD_END:
                    {
                        //Should be the end of a quoted field.
                        //There should only be whitespace or seperator characters next. Else throw. 
                        //This avoids the need for a trim in this mode
                        if (char.IsWhiteSpace(c) && c != DQUOTE && c != SEPARATOR)
                        {
                            readPos++;
                        }
                        else if (c == SEPARATOR)
                        {
                            RecordField(fieldStart, writePos - fieldStart);
                            readPos++;
                            fieldStart = writePos;
                            state = ParseState.FIELD_START;
                        }
                        else
                        {
                            throw new FormatException($"Unexpected characters after quoted field. '{c}' at position {readPos}");
                        }
                    }
                    break;
            }
        }
        switch (state)
        {
            case ParseState.FIELD_START:
                {
                    RecordField(writePos, 0);
                }
                break;

            case ParseState.IN_FIELD_UNQUOTED:
                {
                    int finalFieldLength = writePos - fieldStart;
                    while (finalFieldLength > 0 && char.IsWhiteSpace(chars[fieldStart + finalFieldLength - 1]))
                        finalFieldLength--;
                    RecordField(fieldStart, finalFieldLength);
                }
                break;

            case ParseState.IN_FIELD_QUOTED:
                {
                    throw new FormatException("Unexpected end of input inside a quoted field");
                }

            case ParseState.FIELD_END:
                {
                    RecordField(fieldStart, writePos - fieldStart);
                }
                break;
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
    
    public CsvRowParser(char separator = ',', bool validateRows = false, STRATEGY strategy = STRATEGY.STRICT)
    {
        fieldPositions = ArrayPool<FieldPosition>.Shared.Rent(16);
        chars = ArrayPool<char>.Shared.Rent(64);
        expectedFieldCount = validateRows ? -1 : -2;
        SEPARATOR = separator;
        Parse = strategy switch { STRATEGY.STRICT => ParseStrict, _ => ParseStrict };        
    }

    private struct FieldPosition
    {
        public int Start;
        public int Length;
    }
    private void RecordField(int start, int length)
    {
        fieldPositions[FieldCount++] = new FieldPosition { Start = start, Length = length };
    }    
    public ReadOnlySpan<char> this[int i]
    {
        get { return GetField(i); }
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
    public string[] ToArray()
    {
        string[] result = new string[FieldCount];
        for(int i = 0; i<FieldCount; i++)
        {
            result[i] = GetField(i).ToString();
        }
        return result;
    }

    public void Dispose()
    {
        ArrayPool<FieldPosition>.Shared.Return(fieldPositions, clearArray: true);
        ArrayPool<char>.Shared.Return(chars, clearArray: false);
    }
}

