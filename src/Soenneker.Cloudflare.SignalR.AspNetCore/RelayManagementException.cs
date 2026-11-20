namespace Soenneker.Cloudflare.SignalR.AspNetCore;

/// <summary>Reports the outcome of a management operation for each relay contacted.</summary>
public sealed class RelayManagementException : Exception
{
    internal RelayManagementException(IReadOnlyList<Uri> succeeded, IReadOnlyDictionary<Uri, Exception> failed)
        : base($"Management operation failed on {string.Join(", ", failed.Keys.Select(uri => uri.Authority))}.", new AggregateException(failed.Values))
    {
        Succeeded = succeeded;
        Failed = failed;
    }

    /// <summary>Relay origins that acknowledged the operation.</summary>
    public IReadOnlyList<Uri> Succeeded { get; }

    /// <summary>Relay origins that failed, including requests whose delivery outcome is unknown.</summary>
    public IReadOnlyDictionary<Uri, Exception> Failed { get; }
}
