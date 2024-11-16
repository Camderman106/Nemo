using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Nemo.Model.Components;

internal interface IScalarOutputToString
{
    bool IsOutput { get; }
    string OutputName { get; }
    string OutputToString();
}
[DebuggerDisplay("{Name}: {typeof(T)} {_value}")]

public class Scalar<T> : ScalarBase, IScalarOutputToString, IModelComponent where T : notnull
{
    private bool _calculated;
    private T _value;
    private Func<T>? _evaluationFunction;

    bool IScalarOutputToString.IsOutput => base.IsOutput;


    public Scalar(string outputName, Func<T> evaluationFunction)
    {
        OutputName = outputName;
        _evaluationFunction = evaluationFunction;
        _calculated = false;
        _value = GetDefaultValue();
        Validate();
    }

    public Scalar(string outputName)
    {
        OutputName = outputName;
        _calculated = false;
        _value = GetDefaultValue();
        Validate();
    }

    private void Validate()
    {
        if (typeof(T) != typeof(string) && typeof(T) != typeof(int) && typeof(T) != typeof(double))
        {
            throw new ScalarException($"Unsupported Scalar Type in '{OutputName}'. Type '{typeof(T).Name}'");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public T GetValue()
    {
        if (_calculated)
        {
            return _value;
        }
        else
        {
            if (_evaluationFunction is not null)
            {
                _value = _evaluationFunction();
                _calculated = true;
                return _value!;
            }
            else
            {
                _value = GetDefaultValue();
                _calculated = true;
                return _value;
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static implicit operator T(Scalar<T> instance)
    {
        return instance.GetValue();
    }

    internal void SetValue(T value)
    {
        if (value is null) throw new ScalarException("Cannot assign null value to a scalar");
        _value = value;
        _calculated = true;
    }
    public bool IsCalculated() => _calculated;

    public string Name => OutputName;

    public T Peek()
    {
        return _value;
    }
    public string OutputToString()
    {
        Debug.Assert(_value is not null);
        return _value.ToString() ?? string.Empty;
    }

    public void Reset()
    {
        _value = GetDefaultValue();
        _calculated = false;
    }

    private T GetDefaultValue()
    {
        if (typeof(T) == typeof(string))
        {
            return (T)(object)string.Empty;
        }
        else
        {
            return default!;
        }
    }
}

internal class ScalarException : Exception
{
    public ScalarException()
    {
    }

    public ScalarException(string? message) : base(message)
    {
    }

    public ScalarException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}