using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Routing.Matching;
using Microsoft.AspNetCore.SignalR;
using System.Runtime.CompilerServices;

namespace Soenneker.SignalR.Relay.AspNetCore;

internal sealed class RelayNegotiatePolicy(IRelayTunnelDemand demand) : MatcherPolicy, IEndpointSelectorPolicy
{
    private readonly ConditionalWeakTable<Endpoint, Endpoint> _endpoints = new();
    // The SDK substitutes its negotiate endpoint at order 1. Wrap that handler so endpoint authorization still runs first.
    public override int Order => 2;

    public bool AppliesToEndpoints(IReadOnlyList<Endpoint> endpoints) =>
        endpoints.Any(endpoint => endpoint.Metadata.GetMetadata<NegotiateMetadata>() != null);

    public Task ApplyAsync(HttpContext httpContext, CandidateSet candidates)
    {
        for (var i = 0; i < candidates.Count; i++)
        {
            var endpoint = candidates[i].Endpoint;
            if (endpoint is RouteEndpoint && endpoint.Metadata.GetMetadata<NegotiateMetadata>() != null &&
                endpoint.Metadata.GetMetadata<HubMetadata>() != null)
                candidates.ReplaceEndpoint(i, _endpoints.GetValue(endpoint, Wrap), candidates[i].Values);
        }
        return Task.CompletedTask;
    }

    private Endpoint Wrap(Endpoint endpoint)
    {
        var route = (RouteEndpoint)endpoint;
        var hub = demand.For(route.Metadata.GetMetadata<HubMetadata>()!.HubType.Name);
        return new RouteEndpoint(async context =>
        {
            using var lease = hub.Acquire();
            try
            {
                await hub.WaitUntilReadyAsync(context.RequestAborted);
            }
            catch (OperationCanceledException) when (!context.RequestAborted.IsCancellationRequested)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                return;
            }
            await route.RequestDelegate!(context);
            if (context.Response.StatusCode == StatusCodes.Status200OK) hub.AllowClientHandoff();
        }, route.RoutePattern, route.Order, route.Metadata, route.DisplayName);
    }
}
