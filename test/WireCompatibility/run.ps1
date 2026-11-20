[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Image,
    [string]$AdapterVersion
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
    if ($AdapterVersion) { $properties += "-p:AdapterVersion=$AdapterVersion" }
    Write-Host 'Checking the supported SignalR baseline'
    dotnet run --project $project -c Release @properties -- $container
    Write-Host 'Checking the latest stable Microsoft server, management and client packages'
    if ($AdapterVersion) { $properties = @('-p:AdapterVersion=*') }
    dotnet run --project $project -c Release @properties '-p:AzureSignalRVersion=*' '-p:SignalRClientVersion=*' -- $container
}
finally {
    docker rm --force $container | Out-Null
}
