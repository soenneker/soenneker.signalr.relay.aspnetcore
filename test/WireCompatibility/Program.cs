using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Soenneker.SignalR.Relay.AspNetCore;

using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
var token = deadline.Token;
string endpoint = Environment.GetEnvironmentVariable("Relay__Endpoint")!;
string key = Environment.GetEnvironmentVariable("Relay__AccessKey")!;
string container = args.Single();
var builder = WebApplication.CreateBuilder();
builder.WebHost.UseUrls("http://127.0.0.1:0");
string connectionString = $"Endpoint={endpoint};AccessKey={key};Version=1.0;";
bool idleTest = container == "--idle";
#if SOURCE_ADAPTER
if (idleTest)
    builder.Services.AddSignalR().AddSignalRRelay(connectionString, new SignalRRelayOptions { TunnelIdleTimeout = TimeSpan.FromSeconds(2) });
else
#endif
    builder.Services.AddSignalR().AddSignalRRelay(connectionString);
await using var api = builder.Build();
api.MapHub<CompatibilityHub>("/app");
await api.StartAsync(token);

using var http = new HttpClient { BaseAddress = new Uri(endpoint) };
await using var client = new HubConnectionBuilder()
    .WithUrl(api.Urls.Single() + "/app", HttpTransportType.WebSockets)
    .WithAutomaticReconnect(Enumerable.Repeat(TimeSpan.FromSeconds(1), 120).ToArray())
    .Build();
var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
client.Reconnected += _ => { reconnected.TrySetResult(); return Task.CompletedTask; };
var received = Channel.CreateUnbounded<string>();
client.On<string>("Changed", value => received.Writer.TryWrite(value));

if (idleTest)
{
    await WaitForTunnel(ready: false);
    await client.StartAsync(token);
    // Stay connected well past both the idle timeout and the negotiate handoff window.
    await Task.Delay(TimeSpan.FromSeconds(18), token);
    await CheckDelivery("quiet connected client");
    await client.StopAsync(token);
    await WaitForTunnel(ready: false);
    await Task.Delay(TimeSpan.FromSeconds(3), token);
    using var idle = await http.GetAsync("/ready/compatibilityhub", token);
    if ((int)idle.StatusCode != 503) throw new InvalidOperationException("The SDK reopened an idle tunnel.");
    await using var other = new HubConnectionBuilder().WithUrl(api.Urls.Single() + "/app").Build();
    await Task.WhenAll(client.StartAsync(token), other.StartAsync(token));
    await other.StopAsync(token);
    await CheckDelivery("after concurrent cold negotiations");
    await client.StopAsync(token);
    await WaitForTunnel(ready: false);
    var publisher = api.Services.GetRequiredService<IHubContext<CompatibilityHub>>();
    await publisher.Clients.All.SendAsync("Changed", "background publish", token);
    await WaitForTunnel();
    await WaitForTunnel(ready: false);
    Console.WriteLine("Verified that a background publish wakes and releases its tunnel.");
    await api.StopAsync(token);
    return;
}

await WaitForTunnel();
await client.StartAsync(token);
await CheckDelivery("before restart");

var restart = new ProcessStartInfo("docker") { UseShellExecute = false };
foreach (string argument in new[] { "restart", "--time", "0", container })
    restart.ArgumentList.Add(argument);
using var process = Process.Start(restart)!;
await process.WaitForExitAsync(token);
if (process.ExitCode != 0)
    throw new InvalidOperationException("Could not restart the relay container.");

await reconnected.Task.WaitAsync(token);
await CheckDelivery("after reconnect");
await client.StopAsync(token);
await api.StopAsync(token);

async Task WaitForTunnel(bool ready = true)
{
    while (true)
    {
        token.ThrowIfCancellationRequested();
        try
        {
            using var response = await http.GetAsync("/ready/compatibilityhub", token);
            if (ready ? response.IsSuccessStatusCode : (int)response.StatusCode == 503)
                return;
        }
        catch (HttpRequestException) { /* The container may still be starting. */ }
        await Task.Delay(250, token);
    }
}

async Task CheckDelivery(string marker)
{
    await client.InvokeAsync("Subscribe", token);
    var hub = api.Services.GetRequiredService<IHubContext<CompatibilityHub>>();
    await hub.Clients.Group("subscribers").SendAsync("Changed", marker, token);
    if (await received.Reader.ReadAsync(token) != marker)
        throw new InvalidOperationException($"Unexpected group message {marker}.");
    var manager = api.Services.GetRequiredService<ISignalRRelayConnectionManager>();
    if (!await manager.ExistsAsync(nameof(CompatibilityHub), client.ConnectionId!, token))
        throw new InvalidOperationException($"Management lookup failed {marker}.");
    Console.WriteLine($"Verified group delivery and management lookup {marker}.");
}
