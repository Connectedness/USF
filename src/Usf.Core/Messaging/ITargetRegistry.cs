namespace Usf.Core.Messaging;

public interface ITargetRegistry
{
    Target GetRequiredTarget(string name);

    Target<T> GetRequiredTarget<T>(string name);

    bool TryGetTarget(string name, out Target? target);
}
