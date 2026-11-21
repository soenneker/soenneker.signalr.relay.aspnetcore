[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Image,
    [switch]$PublishedAdapter
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$project = Join-Path $PSScriptRoot WireCompatibility.csproj
$container = "signalr-wire-$([guid]::NewGuid())"
$port = 18382
$env:Relay__Endpoint = "http://127.0.0.1:$port"
$env:Relay__AccessKey = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
docker run --detach --name $container --publish "127.0.0.1:${port}:8080" --env Relay__Endpoint --env Relay__AccessKey $Image
try {
    $properties = @()
    if ($PublishedAdapter) { $properties += '-p:PublishedAdapter=true' }
    Write-Host 'Checking the SignalR versions selected by the adapter'
    dotnet run --project $project -c Release @properties -- $container
    if (!$PublishedAdapter) { dotnet run --project $project -c Release --no-build -- --idle }
    Write-Host 'Checking the latest stable Microsoft server, management and client packages'
    dotnet run --project $project -c Release @properties '-p:AzureSignalRVersion=*' -- $container
}
finally {
    docker rm --force $container | Out-Null
}
