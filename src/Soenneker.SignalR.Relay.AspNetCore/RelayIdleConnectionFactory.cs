using Microsoft.AspNetCore.Connections;
using Microsoft.Azure.SignalR;

namespace Soenneker.SignalR.Relay.AspNetCore;

// Keep the SDK's protocol, reconnect and shutdown paths. Park its transport attempts locally while idle.
internal sealed class RelayIdleConnectionFactory(ConnectionFactory transport, IRelayTunnelDemand demand) : Microsoft.Azure.SignalR.IConnectionFactory
{
    private static readonly object OwnerKey = new();

    public async Task<ConnectionContext> ConnectAsync(HubServiceEndpoint endpoint, TransferFormat transferFormat,
        string connectionId, string target, CancellationToken cancellationToken = default)
    {
        var hub = demand.For(endpoint.Hub);
        var container = (ServiceConnectionContainerBase)endpoint.ConnectionContainer!;
        var service = container.ServiceConnections.Single(connection => connection.ConnectionId == connectionId);
        var context = await hub.ConnectAsync(token => transport.ConnectAsync(endpoint, transferFormat, connectionId, target, token), endpoint, service, cancellationToken);
        context.Items[OwnerKey] = hub;
        return context;
    }

    public Task DisposeAsync(ConnectionContext connection)
    {
        if (connection.Items.Remove(OwnerKey, out var owner)) ((RelayHubTunnels)owner!).Forget(connection);
        return transport.DisposeAsync(connection);
    }
}
