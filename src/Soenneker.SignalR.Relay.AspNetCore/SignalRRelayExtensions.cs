using Soenneker.SignalR.Relay.AspNetCore;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.SignalR;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the ASP.NET Core server connection to a SignalR relay.</summary>
public static class SignalRRelayExtensions
{
    /// <summary>Routes hub negotiation and hub messages through the specified relay endpoint.</summary>
    /// <param name="builder">The application server's SignalR builder.</param>
    /// <param name="connectionString">A service connection string containing Endpoint and AccessKey.</param>
    public static ISignalRServerBuilder AddSignalRRelay(this ISignalRServerBuilder builder, string connectionString) =>
        AddSignalRRelay(builder, connectionString, new SignalRRelayOptions());

    /// <summary>Registers a relay with the specified tunnel lifecycle options.</summary>
    public static ISignalRServerBuilder AddSignalRRelay(this ISignalRServerBuilder builder, string connectionString, SignalRRelayOptions relayOptions)
    {
        ArgumentNullException.ThrowIfNull(relayOptions);
        ValidateEndpoint(connectionString);
        RegisterManagement(builder, [connectionString]);
        builder.AddAzureSignalR(options =>
        {
            options.AllowStatefulReconnects = true;
            if (relayOptions.TunnelIdleTimeout.HasValue) options.ServerStickyMode = ServerStickyMode.Required;
            options.ConnectionString = connectionString;
            options.Endpoints = [];
        });
        return RegisterLifetimeManager(builder, relayOptions);
    }

    /// <summary>Opens application tunnels to two independent relays. The SDK negotiates clients on available endpoints.</summary>
    public static ISignalRServerBuilder AddSignalRRelay(this ISignalRServerBuilder builder, string connectionString, string secondConnectionString) =>
        AddSignalRRelay(builder, connectionString, secondConnectionString, new SignalRRelayOptions());

    /// <summary>Registers a relay with the specified tunnel lifecycle options.</summary>
    public static ISignalRServerBuilder AddSignalRRelay(this ISignalRServerBuilder builder, string connectionString, string secondConnectionString, SignalRRelayOptions relayOptions)
    {
        ArgumentNullException.ThrowIfNull(relayOptions);
        var primary = ValidateEndpoint(connectionString);
        var second = ValidateEndpoint(secondConnectionString);
        if (primary.ServerEndpoint == second.ServerEndpoint || primary.ClientEndpoint == second.ClientEndpoint)
            throw new ArgumentException("Use distinct relay endpoints.", nameof(secondConnectionString));

        RegisterManagement(builder, [connectionString, secondConnectionString]);
        builder.AddAzureSignalR(options =>
        {
            options.AllowStatefulReconnects = true;
            if (relayOptions.TunnelIdleTimeout.HasValue) options.ServerStickyMode = ServerStickyMode.Required;
            options.ConnectionString = null;
            options.Endpoints = [primary, second];
        });
        return RegisterLifetimeManager(builder, relayOptions);
    }

    private static void RegisterManagement(ISignalRServerBuilder builder, string[] endpoints)
    {
        builder.Services.AddHttpClient("SignalRRelay").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler { AllowAutoRedirect = false });
        builder.Services.AddSingleton<ISignalRRelayConnectionManager>(provider =>
            new SignalRRelayConnectionManager(endpoints, provider.GetRequiredService<IHttpClientFactory>()));
    }

    private static ISignalRServerBuilder RegisterLifetimeManager(ISignalRServerBuilder builder, SignalRRelayOptions options)
    {
        if (options.TunnelIdleTimeout is TimeSpan timeout)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
            builder.Services.AddSingleton(new SignalRRelayOptions { TunnelIdleTimeout = timeout });
            builder.Services.AddSingleton<IRelayTunnelDemand, RelayTunnelDemand>();
            builder.Services.AddSingleton<ConnectionFactory>();
            builder.Services.Replace(ServiceDescriptor.Singleton<Microsoft.Azure.SignalR.IConnectionFactory, RelayIdleConnectionFactory>());
            builder.Services.AddSingleton<MatcherPolicy, RelayNegotiatePolicy>();
        }

        var contract = typeof(HubLifetimeManager<>);
        var sdk = builder.Services.Last(d => !d.IsKeyedService && d.ServiceType == contract);
        builder.Services.Remove(sdk);
        builder.Services.Add(ServiceDescriptor.DescribeKeyed(contract, typeof(SignalRRelayExtensions), sdk.ImplementationType!, sdk.Lifetime));
        builder.Services.AddSingleton(contract, typeof(RelayHubLifetimeManager<>));
        return builder;
    }

    private static ServiceEndpoint ValidateEndpoint(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        var endpoint = new ServiceEndpoint(connectionString);
        foreach (var uri in new[] { endpoint.ClientEndpoint, endpoint.ServerEndpoint })
            if (uri.Scheme != Uri.UriSchemeHttps && !(uri.Scheme == Uri.UriSchemeHttp && uri.IsLoopback))
                throw new ArgumentException("Public relay endpoints require HTTPS.", nameof(connectionString));
        return endpoint;
    }
}
