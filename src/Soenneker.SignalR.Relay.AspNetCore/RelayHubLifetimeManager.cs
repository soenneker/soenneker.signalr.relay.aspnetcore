using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace Soenneker.SignalR.Relay.AspNetCore;

// Keep application traffic on Microsoft’s multiplexed tunnels. Register result waiters at the relay before invoking clients.
internal sealed class RelayHubLifetimeManager<THub>(
    [FromKeyedServices(typeof(SignalRRelayExtensions))] HubLifetimeManager<THub> sdk,
    ISignalRRelayConnectionManager management, IRelayTunnelDemand? demand = null) : HubLifetimeManager<THub> where THub : Hub
{
    public override Task<T> InvokeConnectionAsync<T>(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken) =>
        management.InvokeAsync<T>(typeof(THub).Name, connectionId, methodName, args, cancellationToken);

    private readonly RelayHubTunnels? _tunnels = demand?.For(typeof(THub).Name);
    private static readonly object LeaseKey = new();

    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        var lease = _tunnels?.Acquire();
        try
        {
            await sdk.OnConnectedAsync(connection);
            if (lease != null) connection.Items[LeaseKey] = lease;
        }
        catch
        {
            lease?.Dispose();
            throw;
        }
    }

    public override async Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        try { await sdk.OnDisconnectedAsync(connection); }
        finally
        {
            if (connection.Items.Remove(LeaseKey, out var value)) ((IDisposable)value!).Dispose();
        }
    }

    private async Task WithTunnelAsync(Func<Task> operation, CancellationToken cancellationToken)
    {
        using var lease = _tunnels?.Acquire();
        if (_tunnels != null) await _tunnels.WaitUntilReadyAsync(cancellationToken);
        await operation();
    }

    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendAllAsync(methodName, args, cancellationToken), cancellationToken);

    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendAllExceptAsync(methodName, args, excludedConnectionIds, cancellationToken), cancellationToken);

    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendConnectionAsync(connectionId, methodName, args, cancellationToken), cancellationToken);

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendConnectionsAsync(connectionIds, methodName, args, cancellationToken), cancellationToken);

    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendGroupAsync(groupName, methodName, args, cancellationToken), cancellationToken);

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendGroupsAsync(groupNames, methodName, args, cancellationToken), cancellationToken);

    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendGroupExceptAsync(groupName, methodName, args, excludedConnectionIds, cancellationToken), cancellationToken);

    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendUserAsync(userId, methodName, args, cancellationToken), cancellationToken);

    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.SendUsersAsync(userIds, methodName, args, cancellationToken), cancellationToken);

    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.AddToGroupAsync(connectionId, groupName, cancellationToken), cancellationToken);

    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken) =>
        WithTunnelAsync(() => sdk.RemoveFromGroupAsync(connectionId, groupName, cancellationToken), cancellationToken);

    public override Task SetConnectionResultAsync(string connectionId, CompletionMessage result) =>
        WithTunnelAsync(() => sdk.SetConnectionResultAsync(connectionId, result), CancellationToken.None);

    public override bool TryGetReturnType(string invocationId, [NotNullWhen(true)] out Type? type) => sdk.TryGetReturnType(invocationId, out type);
}
