using Soenneker.Cloudflare.SignalR.AspNetCore;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Azure.SignalR;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the ASP.NET Core server connection to a Cloudflare SignalR relay.</summary>
public static class CloudflareSignalRExtensions
{
    /// <summary>Routes hub negotiation and hub messages through the specified Cloudflare service endpoint.</summary>
    /// <param name="builder">The application server's SignalR builder.</param>
    /// <param name="connectionString">A service connection string containing Endpoint and AccessKey.</param>
    public static ISignalRServerBuilder AddCloudflareSignalR(this ISignalRServerBuilder builder, string connectionString)
    {
        ValidateEndpoint(connectionString);
        RegisterManagement(builder, [connectionString]);
        builder.AddAzureSignalR(options =>
        {
            options.AllowStatefulReconnects = true;
            options.ConnectionString = connectionString;
            options.Endpoints = [];
        });
        return RegisterLifetimeManager(builder);
    }

    /// <summary>Opens application tunnels to two independent relays. The SDK negotiates clients on available endpoints.</summary>
    public static ISignalRServerBuilder AddCloudflareSignalR(this ISignalRServerBuilder builder, string connectionString, string secondConnectionString)
    {
        var primary = ValidateEndpoint(connectionString);
        var second = ValidateEndpoint(secondConnectionString);
        if (primary.ServerEndpoint == second.ServerEndpoint || primary.ClientEndpoint == second.ClientEndpoint)
            throw new ArgumentException("Use distinct relay endpoints.", nameof(secondConnectionString));

        RegisterManagement(builder, [connectionString, secondConnectionString]);
        builder.AddAzureSignalR(options =>
        {
            options.AllowStatefulReconnects = true;
            options.ConnectionString = null;
            options.Endpoints = [primary, second];
        });
        return RegisterLifetimeManager(builder);
    }

    private static void RegisterManagement(ISignalRServerBuilder builder, string[] endpoints)
    {
        builder.Services.AddHttpClient("CloudflareSignalR").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler { AllowAutoRedirect = false });
        builder.Services.AddSingleton<ICloudflareSignalRConnectionManager>(provider =>
            new CloudflareSignalRConnectionManager(endpoints, provider.GetRequiredService<IHttpClientFactory>()));
    }

    private static ISignalRServerBuilder RegisterLifetimeManager(ISignalRServerBuilder builder)
    {
        var contract = typeof(HubLifetimeManager<>);
        var sdk = builder.Services.Last(d => !d.IsKeyedService && d.ServiceType == contract);
        builder.Services.Remove(sdk);
        builder.Services.Add(ServiceDescriptor.DescribeKeyed(contract, typeof(CloudflareSignalRExtensions), sdk.ImplementationType!, sdk.Lifetime));
        builder.Services.AddSingleton(contract, typeof(CloudflareHubLifetimeManager<>));
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
