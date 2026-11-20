namespace Soenneker.Cloudflare.SignalR.AspNetCore;

/// <summary>Manages connections on every configured relay.</summary>
public interface ICloudflareSignalRConnectionManager
{
    /// <summary>Returns true when a relay confirms ownership and false when every relay confirms absence. Throws RelayManagementException when ownership remains unknown.</summary>
    Task<bool> ExistsAsync(string hub, string connectionId, CancellationToken cancellationToken = default);

    /// <summary>Adds the connection to a group on its owning relay. Repeating this operation is safe.</summary>
    Task AddToGroupAsync(string hub, string connectionId, string group, CancellationToken cancellationToken = default);

    /// <summary>Removes the connection from a group on its owning relay. An absent connection counts as removed.</summary>
    Task RemoveFromGroupAsync(string hub, string connectionId, string group, CancellationToken cancellationToken = default);

    /// <summary>Sends to a single connection after locating its owner. A failed delivery with an unknown outcome must not be retried blindly.</summary>
    Task SendAsync(string hub, string connectionId, string method, object?[] arguments, CancellationToken cancellationToken = default);

    /// <summary>Sends to a group on every relay. Retry the same payload with the same operation ID within five minutes to suppress duplicate delivery on surviving relays.</summary>
    Task SendGroupAsync(string hub, string group, string method, object?[] arguments, Guid operationId, CancellationToken cancellationToken = default);

    /// <summary>Sends to one connection with a caller-provided retry key valid for five minutes on its owning relay.</summary>
    Task SendAsync(string hub, string connectionId, string method, object?[] arguments, Guid operationId, CancellationToken cancellationToken = default);

    /// <summary>Invokes a client on its owning relay and waits for the result. Supply a cancellation deadline for the operation.</summary>
    Task<T> InvokeAsync<T>(string hub, string connectionId, string method, object?[] arguments, CancellationToken cancellationToken = default);

    /// <summary>Invokes once per live connection and operation ID for five minutes. Repeating an identical request returns the cached result. Retryable results are limited to 16 KiB.</summary>
    Task<T> InvokeAsync<T>(string hub, string connectionId, string method, object?[] arguments, Guid operationId, CancellationToken cancellationToken = default);

    /// <summary>Closes a connection wherever it is hosted. Missing connections count as closed.
    /// Failure to contact any relay is reported to the caller so revocation can be retried.</summary>
    Task CloseAsync(string hub, string connectionId, string reason, CancellationToken cancellationToken = default);
}
