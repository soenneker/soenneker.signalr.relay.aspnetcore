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
builder.Services.AddSignalR().AddSignalRRelay($"Endpoint={endpoint};AccessKey={key};Version=1.0;");
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

async Task WaitForTunnel()
{
    while (true)
    {
        token.ThrowIfCancellationRequested();
        try
        {
            using var response = await http.GetAsync("/ready/compatibilityhub", token);
            if (response.IsSuccessStatusCode)
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
