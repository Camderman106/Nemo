namespace Nemo.Model.Components;

public interface IModelComponent
{
    public string Name { get; }
    public void Reset();
}
