namespace Soenneker.SignalR.Relay.AspNetCore;

internal sealed class RelayTunnelLease(Action release) : IDisposable
{
    private Action? _release = release;

    public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
}
