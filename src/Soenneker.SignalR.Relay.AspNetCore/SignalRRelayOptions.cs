namespace Soenneker.SignalR.Relay.AspNetCore;

/// <summary>Controls the lifetime of application-server tunnels to the relay.</summary>
public sealed class SignalRRelayOptions
{
    /// <summary>Time to retain unused application tunnels. Null keeps them connected continuously.</summary>
    public TimeSpan? TunnelIdleTimeout { get; init; }
}
