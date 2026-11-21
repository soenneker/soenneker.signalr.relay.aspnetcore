namespace Soenneker.SignalR.Relay.AspNetCore;

internal interface IRelayTunnelDemand
{
    RelayHubTunnels For(string hub);
}
