using System.Text.Json;
using System.Data.Common;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.IdentityModel.Tokens;
using Microsoft.IdentityModel.JsonWebTokens;
using System.Text;
using Microsoft.Azure.SignalR.Management;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.SignalR;

namespace Soenneker.SignalR.Relay.AspNetCore;

internal sealed class SignalRRelayConnectionManager(string[] connectionStrings, IHttpClientFactory clients) : ISignalRRelayConnectionManager, IDisposable
{
    private readonly Dictionary<string, ServiceManager> _managers = connectionStrings.ToDictionary(endpoint => endpoint,
        endpoint => new ServiceManagerBuilder().WithOptions(options => options.ConnectionString = endpoint).BuildServiceManager());

    public void Dispose()
    {
        foreach (var manager in _managers.Values) manager.Dispose();
    }

    public async Task<bool> ExistsAsync(string hub, string connectionId, CancellationToken cancellationToken = default) =>
        await FindOwner(hub, connectionId, cancellationToken) is not null;

    public Task AddToGroupAsync(string hub, string connectionId, string group, CancellationToken cancellationToken = default) =>
        OnOwner(hub, connectionId, (context, token) => context.Groups.AddToGroupAsync(connectionId, group, token), false, cancellationToken);

    public Task RemoveFromGroupAsync(string hub, string connectionId, string group, CancellationToken cancellationToken = default) =>
        OnOwner(hub, connectionId, (context, token) => context.Groups.RemoveFromGroupAsync(connectionId, group, token), true, cancellationToken);

    public Task SendAsync(string hub, string connectionId, string method, object?[] arguments, CancellationToken cancellationToken = default) =>
        OnOwner(hub, connectionId, (context, token) => context.Clients.Client(connectionId).SendCoreAsync(method, arguments, token), false, cancellationToken);

    public async Task SendAsync(string hub, string connectionId, string method, object?[] arguments, Guid operationId, CancellationToken cancellationToken = default)
    {
        var owner = await FindOwner(hub, connectionId, cancellationToken) ?? throw new KeyNotFoundException("Connection is offline.");
        await SendOnce(owner, hub, "connections", connectionId, method, arguments, operationId, cancellationToken);
    }

    public Task SendGroupAsync(string hub, string group, string method, object?[] arguments, Guid operationId, CancellationToken cancellationToken = default) =>
        OnAll(endpoint => SendOnce(endpoint, hub, "groups", group, method, arguments, operationId, cancellationToken), cancellationToken);

    public async Task<T> InvokeAsync<T>(string hub, string connectionId, string method, object?[] arguments, Guid operationId, CancellationToken cancellationToken = default)
    {
        var owner = await FindOwner(hub, connectionId, cancellationToken) ?? throw new KeyNotFoundException("Connection is offline.");
        var frame = await SendOnce(owner, hub, "connections", connectionId, method, arguments, operationId, cancellationToken, invoke: true);
        if (frame.Length == 0 || frame[^1] != 0x1e) throw new InvalidDataException("Invalid client completion frame.");
        using var document = JsonDocument.Parse(frame.AsMemory(0, frame.Length - 1));
        if (document.RootElement.TryGetProperty("error", out var error)) throw new HubException(error.GetString());
        if (!document.RootElement.TryGetProperty("result", out var result)) throw new HubException("Client returned no result.");
        return JsonSerializer.Deserialize<T>(result, JsonSerializerOptions.Web)!;
    }

    private async Task<byte[]> SendOnce(string endpoint, string hub, string audience, string name, string method, object?[] args, Guid operationId, CancellationToken token, bool invoke = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hub);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        if (operationId == Guid.Empty) throw new ArgumentException("Supply a unique operation ID.", nameof(operationId));
        var origin = new ServiceEndpoint(endpoint).ServerEndpoint;
        string action = invoke ? ":invoke" : ":send";
        var uri = new Uri(origin, $"api/hubs/{Uri.EscapeDataString(hub.ToLowerInvariant())}/{audience}/{Uri.EscapeDataString(name)}/{action}");
        var settings = new DbConnectionStringBuilder { ConnectionString = endpoint };
        var secret = (string)settings["AccessKey"];
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secret));
        var jwt = new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Audience = $"{origin.Scheme}://{origin.Host}{uri.AbsolutePath}",
            Issuer = "azure-signalr",
            Expires = DateTime.UtcNow.AddMinutes(2),
            SigningCredentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256)
        });
        using var request = new HttpRequestMessage(HttpMethod.Post, uri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", jwt);
        request.Headers.Add("Idempotency-Key", operationId.ToString());
        request.Content = JsonContent.Create(new { target = method, arguments = args });
        using var http = clients.CreateClient("SignalRRelay");
        using var response = await http.SendAsync(request, token);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Relay {origin.Authority} returned {(int)response.StatusCode}. Retry with the same operation ID.", null, response.StatusCode);
        return invoke ? await response.Content.ReadAsByteArrayAsync(token) : [];
    }

    public async Task<T> InvokeAsync<T>(string hub, string connectionId, string method, object?[] arguments, CancellationToken cancellationToken = default)
    {
        var owner = await FindOwner(hub, connectionId, cancellationToken) ?? throw new KeyNotFoundException("Connection is offline.");
        await using var context = await _managers[owner].CreateHubContextAsync(hub, cancellationToken);
        return await context.Clients.Client(connectionId).InvokeCoreAsync<T>(method, arguments, cancellationToken);
    }

    private async Task OnOwner(string hub, string connectionId, Func<ServiceHubContext, CancellationToken, Task> action, bool allowAbsent, CancellationToken token)
    {
        var owner = await FindOwner(hub, connectionId, token);
        if (owner is null)
        {
            if (allowAbsent) return;
            throw new KeyNotFoundException("Connection is offline.");
        }
        await using var context = await _managers[owner].CreateHubContextAsync(hub, token);
        await action(context, token);
    }

    private async Task<string?> FindOwner(string hub, string id, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hub);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        token.ThrowIfCancellationRequested();
        using var probes = CancellationTokenSource.CreateLinkedTokenSource(token);
        var pending = connectionStrings.Select(endpoint => ProbeOwner(endpoint, hub, id, probes.Token)).ToList();
        var checkedOrigins = new List<Uri>();
        var failures = new Dictionary<Uri, Exception>();
        try
        {
            while (pending.Count > 0)
            {
                var completed = await Task.WhenAny(pending).WaitAsync(token);
                pending.Remove(completed);
                var (endpoint, ownsConnection, error) = await completed;
                token.ThrowIfCancellationRequested();
                if (ownsConnection) return endpoint;

                var origin = new ServiceEndpoint(endpoint).ServerEndpoint;
                if (error is null) checkedOrigins.Add(origin);
                else failures.Add(origin, error);
            }
        }
        finally
        {
            // Relay-generated connection IDs identify a single owner. A slow replica must not delay delivery there.
            if (pending.Count > 0) probes.Cancel();
        }

        if (failures.Count > 0) throw new RelayManagementException(checkedOrigins, failures);
        return null;
    }

    private async Task<(string Endpoint, bool OwnsConnection, Exception? Error)> ProbeOwner(string endpoint, string hub, string id, CancellationToken token)
    {
        try
        {
            await using var context = await _managers[endpoint].CreateHubContextAsync(hub, token);
            return (endpoint, await context.ClientManager.ConnectionExistsAsync(id, token), null);
        }
        catch (Exception error)
        {
            // The coordinator honors caller cancellation and can stop outstanding probes after a positive answer.
            return (endpoint, false, error);
        }
    }

    private async Task OnAll(Func<string, Task> action, CancellationToken token)
    {
        var outcomes = await Task.WhenAll(connectionStrings.Select(async endpoint =>
        {
            var origin = new ServiceEndpoint(endpoint).ServerEndpoint;
            try { await action(endpoint); return (Origin: origin, Error: (Exception?)null); }
            catch (Exception error) when (!token.IsCancellationRequested) { return (Origin: origin, Error: error); }
        }));
        var failures = outcomes.Where(o => o.Error != null).ToDictionary(o => o.Origin, o => o.Error!);
        if (failures.Count > 0)
            throw new RelayManagementException(outcomes.Where(o => o.Error == null).Select(o => o.Origin).ToArray(), failures);
    }

    public Task CloseAsync(string hub, string connectionId, string reason, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hub);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionId);
        return OnAll(endpoint => CloseOnEndpoint(endpoint, hub, connectionId, reason, cancellationToken), cancellationToken);
    }

    private async Task CloseOnEndpoint(string endpoint, string hub, string connectionId, string reason, CancellationToken token)
    {
        await using var context = await _managers[endpoint].CreateHubContextAsync(hub, token);
        await context.ClientManager.CloseConnectionAsync(connectionId, reason, token);
    }
}
