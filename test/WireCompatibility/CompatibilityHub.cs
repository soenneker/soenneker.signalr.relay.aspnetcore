using Microsoft.AspNetCore.SignalR;

public sealed class CompatibilityHub : Hub
{
    public Task Subscribe() => Groups.AddToGroupAsync(Context.ConnectionId, "subscribers");
}
