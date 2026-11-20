using Microsoft.AspNetCore.Builder;
using Microsoft.Azure.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Soenneker.SignalR.Relay.AspNetCore.Tests;

public sealed class SignalRRelayTests
{
    [Test]
    public async Task RegistersOnlyTheSpecifiedRelayDuringMigration()
    {
        const string relay = "Endpoint=https://relay.example.com;AccessKey=example-test-key-0000000000000000000;";
        var app = WebApplication.CreateBuilder();
        app.Configuration["Azure:SignalR:ConnectionString"] = "Endpoint=https://legacy.example.com;AccessKey=example-test-key-1111111111111111111;";
        app.Services.AddSignalR().AddSignalRRelay(relay);
        await using var host = app.Build();
        var options = host.Services.GetRequiredService<IOptions<ServiceOptions>>().Value;
        await Assert.That(options.ConnectionString).IsEqualTo(relay);
        await Assert.That(options.Endpoints?.Length).IsEqualTo(0);
    }
}
