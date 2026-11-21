using Microsoft.AspNetCore.Connections;
using Microsoft.Azure.SignalR;
using System.Diagnostics;

namespace Soenneker.SignalR.Relay.AspNetCore;

internal sealed class RelayHubTunnels(TimeSpan timeout, CancellationToken stopping) : IDisposable
{
    private readonly Lock _gate = new();
    private readonly Dictionary<ConnectionContext, IServiceConnection> _connections = [];
    private readonly HashSet<HubServiceEndpoint> _endpoints = [];
    private readonly HashSet<HubServiceEndpoint> _attempted = [];
    private TaskCompletionSource _connected = NewSignal();
    private TaskCompletionSource _wake = NewSignal();
    private int _users;
    private Timer? _timer;
    private long _released;
    private long? _negotiated;
    private bool _awake;

    // A redirect and its client WebSocket arrive on separate requests.
    public void AllowClientHandoff()
    {
        lock (_gate) _negotiated = Stopwatch.GetTimestamp();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IDisposable Acquire()
    {
        lock (_gate)
        {
            stopping.ThrowIfCancellationRequested();
            _users++;
            _awake = true;
            _wake.TrySetResult();
            return new RelayTunnelLease(Release);
        }
    }

    private void Release()
    {
        lock (_gate)
        {
            if (--_users != 0) return;
            _released = Stopwatch.GetTimestamp();
            _timer ??= new Timer(_ => Expire());
            Schedule(timeout);
        }
    }

    private void Schedule(TimeSpan delay) =>
        _timer!.Change(TimeSpan.FromMilliseconds(Math.Clamp(delay.TotalMilliseconds, 1, int.MaxValue)), Timeout.InfiniteTimeSpan);

    private void Expire()
    {
        lock (_gate)
        {
            if (_users != 0 || !_awake) return;
            var remaining = timeout - Stopwatch.GetElapsedTime(_released);
            if (_negotiated is long redirected)
            {
                var grace = TimeSpan.FromSeconds(15) - Stopwatch.GetElapsedTime(redirected);
                if (grace > remaining) remaining = grace;
            }
            if (remaining > TimeSpan.Zero)
            {
                Schedule(remaining);
                return;
            }

            _awake = false;
            _wake = NewSignal();
            _attempted.Clear();
            foreach (var (context, connection) in _connections)
            {
                _ = connection.StopAsync();
                if (connection.Status == ServiceConnectionStatus.Connecting) context.Transport.Input.CancelPendingRead();
            }
            _connections.Clear();
        }
    }

    public async Task<ConnectionContext> ConnectAsync(Func<CancellationToken, Task<ConnectionContext>> connect,
        HubServiceEndpoint endpoint, IServiceConnection service, CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stopping);
        Task generation;
        lock (_gate)
        {
            if (_endpoints.Add(endpoint)) endpoint.Online = false;
            generation = _wake.Task;
        }
        await generation.WaitAsync(linked.Token);
        ConnectionContext context;
        try { context = await connect(linked.Token); }
        catch
        {
            MarkAttempted(endpoint, generation);
            throw;
        }
        lock (_gate)
        {
            if (_awake && generation == _wake.Task)
            {
                _connections.Add(context, service);
                _ = ObserveHandshakeAsync(endpoint, service, generation);
            }
            else
            {
                // The socket opened after its demand expired. Interrupt its pending SDK handshake.
                context.Transport.Input.CancelPendingRead();
            }
        }
        return context;
    }

    private async Task ObserveHandshakeAsync(HubServiceEndpoint endpoint, IServiceConnection service, Task generation)
    {
        await service.ConnectionInitializedTask;
        MarkAttempted(endpoint, generation);
    }

    private void MarkAttempted(HubServiceEndpoint endpoint, Task generation)
    {
        lock (_gate)
        {
            if (generation != _wake.Task) return;
            _attempted.Add(endpoint);
            SignalChanged();
        }
    }

    public void Forget(ConnectionContext context)
    {
        lock (_gate) _connections.Remove(context);
    }

    private void SignalChanged()
    {
        _connected.TrySetResult();
        _connected = NewSignal();
    }

    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        Task changed;
        lock (_gate)
        {
            if (HasReadyConnection()) return;
            changed = _connected.Task;
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stopping);
        deadline.CancelAfter(TimeSpan.FromMinutes(1));
        while (true)
        {
            await changed.WaitAsync(deadline.Token);
            lock (_gate)
            {
                if (HasReadyConnection()) return;
                changed = _connected.Task;
            }
        }
    }

    private bool HasReadyConnection() => _attempted.Count == _endpoints.Count &&
        _connections.Values.Any(connection => connection.Status == ServiceConnectionStatus.Connected);

    public void Dispose()
    {
        lock (_gate) _timer?.Dispose();
    }
}
