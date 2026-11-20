using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace Soenneker.SignalR.Relay.AspNetCore;

// Keep application traffic on Microsoft’s multiplexed tunnels. Register result waiters at the relay before invoking clients.
internal sealed class RelayHubLifetimeManager<THub>(
    [FromKeyedServices(typeof(SignalRRelayExtensions))] HubLifetimeManager<THub> sdk,
    ISignalRRelayConnectionManager management) : HubLifetimeManager<THub> where THub : Hub
{
    public override Task<T> InvokeConnectionAsync<T>(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken) =>
        management.InvokeAsync<T>(typeof(THub).Name, connectionId, methodName, args, cancellationToken);

    public override Task OnConnectedAsync(HubConnectionContext connection) =>
        sdk.OnConnectedAsync(connection);

    public override Task OnDisconnectedAsync(HubConnectionContext connection) =>
        sdk.OnDisconnectedAsync(connection);

    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendAllAsync(methodName, args, cancellationToken);

    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken) =>
        sdk.SendAllExceptAsync(methodName, args, excludedConnectionIds, cancellationToken);

    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendConnectionAsync(connectionId, methodName, args, cancellationToken);

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendConnectionsAsync(connectionIds, methodName, args, cancellationToken);

    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendGroupAsync(groupName, methodName, args, cancellationToken);

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendGroupsAsync(groupNames, methodName, args, cancellationToken);

    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken) =>
        sdk.SendGroupExceptAsync(groupName, methodName, args, excludedConnectionIds, cancellationToken);

    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendUserAsync(userId, methodName, args, cancellationToken);

    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args, CancellationToken cancellationToken) =>
        sdk.SendUsersAsync(userIds, methodName, args, cancellationToken);

    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken) =>
        sdk.AddToGroupAsync(connectionId, groupName, cancellationToken);

    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken) =>
        sdk.RemoveFromGroupAsync(connectionId, groupName, cancellationToken);

    public override Task SetConnectionResultAsync(string connectionId, CompletionMessage result) =>
        sdk.SetConnectionResultAsync(connectionId, result);

    public override bool TryGetReturnType(string invocationId, [NotNullWhen(true)] out Type? type) => sdk.TryGetReturnType(invocationId, out type);
}
