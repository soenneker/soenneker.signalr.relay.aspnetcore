using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Soenneker.Cloudflare.SignalR.AspNetCore;

namespace Soenneker.Cloudflare.SignalR.AspNetCore.Tests;

public sealed class MultiRelayManagementTests
{
    [Test]
    public async Task RevokeContactsBothRelaysAndReportsAnUnavailableEndpoint()
    {
        int firstCalls = 0, secondCalls = 0;
        await using var first = await StartEndpoint(() => Interlocked.Increment(ref firstCalls));
        await using var second = await StartEndpoint(() => Interlocked.Increment(ref secondCalls));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var builder = WebApplication.CreateBuilder();
        var firstKey = "Endpoint=" + first.Urls.Single() + ";AccessKey=local-test-endpoint-key-000000000000000001;";
        var secondKey = "Endpoint=" + second.Urls.Single() + ";AccessKey=local-test-endpoint-key-000000000000000002;";
        builder.Services.AddSignalR().AddCloudflareSignalR(firstKey, secondKey);
        await using var api = builder.Build();
        var endpoints = api.Services.GetRequiredService<IOptions<ServiceOptions>>().Value.Endpoints!;
        await Assert.That(endpoints.Length).IsEqualTo(2);
        await Assert.That(endpoints.All(e => e.EndpointType == EndpointType.Primary)).IsTrue();
        var manager = api.Services.GetRequiredService<ICloudflareSignalRConnectionManager>();
        try
        {
            await manager.CloseAsync("apphub", "absent", "revoked", timeout.Token);
            await Assert.That(firstCalls).IsEqualTo(1);
            await Assert.That(secondCalls).IsEqualTo(1);
            await second.StopAsync();
            bool failed = false;
            try { await manager.CloseAsync("apphub", "absent", "revoked", timeout.Token); }
            catch (RelayManagementException error)
            {
                failed = error.Succeeded.Single().Authority == new Uri(first.Urls.Single()).Authority
                    && error.Failed.Keys.Single().Authority == endpoints[1].ServerEndpoint.Authority;
            }
            await Assert.That(failed).IsTrue();
            await Assert.That(firstCalls).IsEqualTo(2);
        }
        finally { await first.StopAsync(); await second.StopAsync(); }
    }

    [Test]
    public async Task PositiveLookupSurvivesAStalledOrOfflineRelay()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        var stalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var owner = await StartEndpoint(() => { }, async (_, id) =>
        {
            if (id == "slow-check") await stalled.Task.WaitAsync(token);
            return id != "absent";
        });
        await using var replica = await StartEndpoint(() => { }, async (http, id) =>
        {
            if (id == "slow-check")
            {
                stalled.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, http.RequestAborted); }
                finally { canceled.TrySetResult(); }
            }
            return false;
        });
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddSignalR().AddCloudflareSignalR(
            $"Endpoint={replica.Urls.Single()};AccessKey=local-test-endpoint-key-000000000000000001;",
            $"Endpoint={owner.Urls.Single()};AccessKey=local-test-endpoint-key-000000000000000002;");
        await using var api = builder.Build();
        var manager = api.Services.GetRequiredService<ICloudflareSignalRConnectionManager>();
        try
        {
            await Assert.That(await manager.ExistsAsync("apphub", "slow-check", token)).IsTrue();
            await canceled.Task.WaitAsync(token);
            await Assert.That(await manager.ExistsAsync("apphub", "absent", token)).IsFalse();
            await replica.StopAsync(token);
            await Assert.That(await manager.ExistsAsync("apphub", "online", token)).IsTrue();
            try
            {
                await manager.RemoveFromGroupAsync("apphub", "absent", "group", token);
                throw new InvalidOperationException("Expected an indeterminate lookup.");
            }
            catch (RelayManagementException error)
            {
                await Assert.That(error.Succeeded.Single().Authority).IsEqualTo(new Uri(owner.Urls.Single()).Authority);
                await Assert.That(error.Failed.Count).IsEqualTo(1);
            }
        }
        finally { await owner.StopAsync(); await replica.StopAsync(); }
    }

    private static async Task<WebApplication> StartEndpoint(Action recordDelete, Func<HttpContext, string, Task<bool>>? lookup = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var app = builder.Build();
        app.MapGet("/api/health", () => Results.Ok());
        app.MapMethods("/api/hubs/apphub/connections/{id}", ["HEAD"], async (HttpContext http, string id) =>
        {
            if (lookup is not null && await lookup(http, id)) return Results.Ok();
            http.Response.Headers["x-ms-error-code"] = "Warning.Connection.NotExisted";
            return Results.NotFound();
        });
        app.MapDelete("/api/hubs/apphub/connections/absent", (HttpContext http) =>
        {
            if (!http.Request.Headers.Authorization.ToString().StartsWith("Bearer ", StringComparison.Ordinal))
                return Results.Unauthorized();
            recordDelete();
            http.Response.Headers["x-ms-error-code"] = "Warning.Connection.NotExisted";
            return Results.NotFound();
        });
        await app.StartAsync();
        return app;
    }
}
