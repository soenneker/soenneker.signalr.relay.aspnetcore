using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;

namespace Soenneker.SignalR.Relay.AspNetCore;

internal sealed class RelayTunnelDemand(SignalRRelayOptions options, IHostApplicationLifetime lifetime) : IRelayTunnelDemand, IDisposable
{
    private readonly ConcurrentDictionary<string, RelayHubTunnels> _hubs = new(StringComparer.OrdinalIgnoreCase);

    public RelayHubTunnels For(string hub) => _hubs.GetOrAdd(hub, _ => new RelayHubTunnels(options.TunnelIdleTimeout!.Value, lifetime.ApplicationStopping));

    public void Dispose()
    {
        foreach (var hub in _hubs.Values) hub.Dispose();
    }
}
