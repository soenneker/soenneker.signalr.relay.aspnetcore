# Soenneker.SignalR.Relay.AspNetCore

ASP.NET Core server registration for Soenneker.SignalR.Relay. The relay holds client connections. Hub classes, authorization and publishers remain in the application.

```csharp
builder.Services.AddSignalR()
    .AddSignalRRelay(builder.Configuration.GetConnectionString("SignalRRelay")
        ?? throw new InvalidOperationException("SignalRRelay connection string is required."));

app.MapHub<AppHub>("/hubs/app").RequireAuthorization();
```

Set the connection string to `Endpoint=https://relay.example.com;AccessKey=<secret>;Version=1.0;`. Use the same secret as the relay's `Relay__AccessKey`. Keep it in server-side configuration. Portal, mobile and other SignalR clients continue to use the mapped application hub URL.

The package reuses the Microsoft.Azure.SignalR 1.34.0 SDK for negotiation, authentication and application-server tunnels. The configured endpoint directs those tunnels to the Cloudflare-hosted relay.

## Two relay endpoints

```csharp
builder.Services.AddSignalR().AddSignalRRelay(firstConnectionString, secondConnectionString);
```

Configure every API instance with the same pair of independently keyed relay origins. The SDK opens tunnels to both and negotiates clients on available endpoints. Clients with automatic reconnect can renegotiate on a surviving endpoint. Restore membership in the hub connection handler and refresh data missed while disconnected.

For session revocation, inject `ISignalRRelayConnectionManager` and await `CloseAsync(hub, connectionId, reason, cancellationToken)`. It issues a close on each endpoint. If any endpoint is unreachable, the call reports failure so the application can retry before releasing its connection leases.

Clients opt into same-session WebSocket recovery with `WithStatefulReconnect()` in .NET or `withStatefulReconnect()` in JavaScript. Use automatic reconnect as well for a new session when the original relay has stopped.

The management interface locates the owning relay for connection lookup, membership changes, targeted sends and client invocations. Application `IHubContext` client-result calls register the result waiter at the owning relay before sending the invocation. Management fanout failures expose successful and failed relay origins through `RelayManagementException`.

Pass one stable operation GUID to `SendGroupAsync`, or the keyed overload of `SendAsync` or `InvokeAsync<T>`, when retrying an operation after a lost response. Repeating the same request within five minutes suppresses duplicate execution on the surviving relay. Reusing a key with different arguments returns a conflict. Keyed invocation results are limited to 16 KiB.
