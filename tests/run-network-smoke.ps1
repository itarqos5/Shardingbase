[CmdletBinding()]
param(
    [string] $ServerJar = (Join-Path $PSScriptRoot '..\build\release\server.jar'),
    [string] $VelocityPluginJar = (Join-Path $PSScriptRoot '..\build\release\shardingbase-velocity.jar'),
    [int] $StartupTimeoutSeconds = 240
)

$ErrorActionPreference = 'Stop'
$testsRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$cacheRoot = Join-Path $testsRoot 'cache'
$runtimeRoot = Join-Path $testsRoot 'runtime'
$resultsRoot = Join-Path $testsRoot 'results'
$serverJarPath = [System.IO.Path]::GetFullPath($ServerJar)
$velocityPluginPath = [System.IO.Path]::GetFullPath($VelocityPluginJar)
$java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\java.exe' } else { $null }
if (-not $java -or -not (Test-Path -LiteralPath $java -PathType Leaf)) {
    $java = (Get-Command java.exe -ErrorAction Stop).Source
}
foreach ($artifact in @($serverJarPath, $velocityPluginPath)) {
    if (-not (Test-Path -LiteralPath $artifact -PathType Leaf)) {
        throw "Release artifact not found: $artifact"
    }
}

$velocityUrl = 'https://fill-data.papermc.io/v1/objects/846411d2d0560fed0f23496ffb89681be528d2c0650ecdcf21724d2d7bd9c1ee/velocity-4.1.1-24.jar'
$velocitySha256 = '846411d2d0560fed0f23496ffb89681be528d2c0650ecdcf21724d2d7bd9c1ee'
$velocityJar = Join-Path $cacheRoot 'velocity-4.1.1-24.jar'
New-Item -ItemType Directory -Force -Path $cacheRoot, $runtimeRoot, $resultsRoot | Out-Null
if (-not (Test-Path -LiteralPath $velocityJar -PathType Leaf) -or
    (Get-FileHash -LiteralPath $velocityJar -Algorithm SHA256).Hash.ToLowerInvariant() -ne $velocitySha256) {
    Invoke-WebRequest -Uri $velocityUrl -OutFile $velocityJar -Headers @{
        'User-Agent' = 'Shardingbase-network-smoke/1.0'
    }
}
if ((Get-FileHash -LiteralPath $velocityJar -Algorithm SHA256).Hash.ToLowerInvariant() -ne $velocitySha256) {
    throw 'Velocity SHA-256 verification failed'
}

function New-Token([int] $Bytes) {
    $buffer = [byte[]]::new($Bytes)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($buffer)
    return [Convert]::ToBase64String($buffer).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

$reservedPorts = [System.Collections.Generic.HashSet[int]]::new()
function Get-FreeTcpPort {
    do {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
        $listener.Start()
        try {
            $candidate = ([System.Net.IPEndPoint] $listener.LocalEndpoint).Port
        } finally {
            $listener.Stop()
        }
    } while (-not $reservedPorts.Add($candidate))
    return $candidate
}

function Start-CapturedJava([string] $Directory, [string[]] $Arguments, [hashtable] $Environment) {
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $java
    $startInfo.WorkingDirectory = $Directory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardInput = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        [void] $startInfo.ArgumentList.Add($argument)
    }
    foreach ($entry in $Environment.GetEnumerator()) {
        $startInfo.Environment[$entry.Key] = $entry.Value
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    [void] $process.Start()
    return [pscustomobject]@{
        Process = $process
        StandardOutput = $process.StandardOutput.ReadToEndAsync()
        StandardError = $process.StandardError.ReadToEndAsync()
    }
}

function Wait-TcpPort([int] $Port, [DateTime] $Deadline) {
    while ([DateTime]::UtcNow -lt $Deadline) {
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $client.Connect('127.0.0.1', $Port)
            $client.Dispose()
            return
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }
    throw "TCP port $Port did not open before the startup deadline"
}

function Wait-Log([string] $Path, [string] $Pattern, [DateTime] $Deadline) {
    while ([DateTime]::UtcNow -lt $Deadline) {
        if (Test-Path -LiteralPath $Path -PathType Leaf) {
            $content = Get-Content -Raw -LiteralPath $Path
            if ($content -match $Pattern) {
                return $content
            }
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Log did not contain '$Pattern' before the startup deadline: $Path"
}

$runId = 'network-{0}-{1}' -f (Get-Date -Format 'yyyyMMdd-HHmmss'), ([guid]::NewGuid().ToString('N').Substring(0, 8))
$runDirectory = [System.IO.Path]::GetFullPath((Join-Path $runtimeRoot $runId))
if (-not $runDirectory.StartsWith([System.IO.Path]::GetFullPath($runtimeRoot), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing unsafe runtime path: $runDirectory"
}
$proxyDirectory = Join-Path $runDirectory 'velocity'
$nodeADirectory = Join-Path $runDirectory 'node-a'
$nodeBDirectory = Join-Path $runDirectory 'node-b'
$requiredDirectories = @(
    (Join-Path $proxyDirectory 'plugins\shardingbase')
    $nodeADirectory
    $nodeBDirectory
)
New-Item -ItemType Directory -Force -Path $requiredDirectories | Out-Null
Copy-Item -LiteralPath $velocityJar -Destination (Join-Path $proxyDirectory 'velocity.jar')
Copy-Item -LiteralPath $velocityPluginPath -Destination (Join-Path $proxyDirectory 'plugins\shardingbase-velocity.jar')
Copy-Item -LiteralPath $serverJarPath -Destination (Join-Path $nodeADirectory 'server.jar')
Copy-Item -LiteralPath $serverJarPath -Destination (Join-Path $nodeBDirectory 'server.jar')

$proxyPort = Get-FreeTcpPort
$controlPort = Get-FreeTcpPort
$webPort = Get-FreeTcpPort
$nodeAPort = Get-FreeTcpPort
$nodeBPort = Get-FreeTcpPort
$credentialA = New-Token 32
$credentialB = New-Token 32
$signingKey = New-Token 32
$keyStorePassword = New-Token 24

@"
config-version = "2.7"
bind = "127.0.0.1:$proxyPort"
motd = "Shardingbase network smoke"
show-max-players = 20
online-mode = false
force-key-authentication = false
prevent-client-proxy-connections = false
player-info-forwarding-mode = "none"
announce-forge = false
kick-existing-players = false
ping-passthrough = "DISABLED"
enable-player-address-logging = false

[servers]
shard-a = "127.0.0.1:$nodeAPort"
shard-b = "127.0.0.1:$nodeBPort"
try = ["shard-a"]

[forced-hosts]

[advanced]
compression-threshold = 256
compression-level = -1
login-ratelimit = 0
connection-timeout = 5000
read-timeout = 30000
haproxy-protocol = false
tcp-fast-open = false
bungee-plugin-message-channel = true
show-ping-requests = false
failover-on-unexpected-server-disconnect = true
announce-proxy-commands = true
log-command-executions = false
log-player-connections = false
accepts-transfers = false
enable-reuse-port = false

[query]
enabled = false
port = $proxyPort
map = "Velocity"
show-plugins = false
"@ | Set-Content -LiteralPath (Join-Path $proxyDirectory 'velocity.toml') -Encoding utf8NoBOM

@"
control:
  bind: "127.0.0.1"
  port: $controlPort
  keystore: "tls.p12"
  keystore-password: "$keyStorePassword"
  transaction-signing-key: "$signingKey"
database: "shardingbase.db"
node-credentials:
  node-a: "$credentialA"
  node-b: "$credentialB"
remote-command-allowlist: []
web:
  bind: "127.0.0.1"
  port: $webPort
  public-url: "https://127.0.0.1:$webPort"
"@ | Set-Content -LiteralPath (Join-Path $proxyDirectory 'plugins\shardingbase\config.yml') -Encoding utf8NoBOM

foreach ($node in @(
    @{ Directory = $nodeADirectory; Id = 'backend-a'; Name = 'shard-a'; Port = $nodeAPort },
    @{ Directory = $nodeBDirectory; Id = 'backend-b'; Name = 'shard-b'; Port = $nodeBPort }
)) {
    New-Item -ItemType Directory -Force -Path (Join-Path $node.Directory 'config') | Out-Null
    Set-Content -LiteralPath (Join-Path $node.Directory 'eula.txt') -Value 'eula=true' -Encoding utf8NoBOM
    @(
        'online-mode=false'
        'server-ip=127.0.0.1'
        "server-port=$($node.Port)"
        'level-name=world'
        'spawn-protection=0'
        'view-distance=2'
        'simulation-distance=2'
        "motd=Shardingbase $($node.Name)"
    ) | Set-Content -LiteralPath (Join-Path $node.Directory 'server.properties') -Encoding ascii
    @"
server-id: "$($node.Id)"
server-name: "$($node.Name)"
"@ | Set-Content -LiteralPath (Join-Path $node.Directory 'config\shardingbase.yml') -Encoding utf8NoBOM
}

$proxy = $null
$nodeA = $null
$nodeB = $null
$passed = $false
$failure = $null
try {
    $deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
    $proxy = Start-CapturedJava $proxyDirectory @('-Xms128M', '-Xmx512M', '-jar', 'velocity.jar') @{}
    Wait-TcpPort $controlPort $deadline
    $keyStorePath = Join-Path $proxyDirectory 'plugins\shardingbase\tls.p12'
    while (-not (Test-Path -LiteralPath $keyStorePath -PathType Leaf) -and [DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 250
    }
    if (-not (Test-Path -LiteralPath $keyStorePath -PathType Leaf)) {
        throw 'Velocity did not generate the controller TLS key store'
    }
    $certificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
        $keyStorePath,
        $keyStorePassword,
        [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet
    )
    try {
        $fingerprint = $certificate.GetCertHashString([System.Security.Cryptography.HashAlgorithmName]::SHA256)
    } finally {
        $certificate.Dispose()
    }

    $commonEnvironment = @{
        SHARDINGBASE_CONTROLLER_URI = "tls://127.0.0.1:$controlPort"
        SHARDINGBASE_CERTIFICATE_SHA256 = $fingerprint
        SHARDINGBASE_BACKEND_MEMORY_MB = '768'
        SHARDINGBASE_TRANSACTION_KEY = $signingKey
    }
    $environmentA = $commonEnvironment.Clone()
    $environmentA.SHARDINGBASE_NODE_ID = 'node-a'
    $environmentA.SHARDINGBASE_NODE_CREDENTIAL = $credentialA
    $environmentA.SHARDINGBASE_WORLD_ROOT = $nodeADirectory
    $environmentA.SHARDINGBASE_BACKUP_ROOT = (Join-Path $nodeADirectory 'backups')
    $environmentA.SHARDINGBASE_TRANSACTION_ROOT = (Join-Path $nodeADirectory 'transactions')
    $environmentA.SHARDINGBASE_STAGING_ROOT = (Join-Path $nodeADirectory 'staging')
    $environmentB = $commonEnvironment.Clone()
    $environmentB.SHARDINGBASE_NODE_ID = 'node-b'
    $environmentB.SHARDINGBASE_NODE_CREDENTIAL = $credentialB
    $environmentB.SHARDINGBASE_WORLD_ROOT = $nodeBDirectory
    $environmentB.SHARDINGBASE_BACKUP_ROOT = (Join-Path $nodeBDirectory 'backups')
    $environmentB.SHARDINGBASE_TRANSACTION_ROOT = (Join-Path $nodeBDirectory 'transactions')
    $environmentB.SHARDINGBASE_STAGING_ROOT = (Join-Path $nodeBDirectory 'staging')

    $nodeA = Start-CapturedJava $nodeADirectory @('-Xms64M', '-Xmx128M', '-jar', 'server.jar', 'nogui') $environmentA
    $nodeB = Start-CapturedJava $nodeBDirectory @('-Xms64M', '-Xmx128M', '-jar', 'server.jar', 'nogui') $environmentB
    $logA = Wait-Log (Join-Path $nodeADirectory 'logs\latest.log') 'Shardingbase features enabled for shard-a' $deadline
    $logB = Wait-Log (Join-Path $nodeBDirectory 'logs\latest.log') 'Shardingbase features enabled for shard-b' $deadline
    if ($logA -notmatch 'Done \([0-9.]+s\)! For help') {
        $logA = Wait-Log (Join-Path $nodeADirectory 'logs\latest.log') 'Done \([0-9.]+s\)! For help' $deadline
    }
    if ($logB -notmatch 'Done \([0-9.]+s\)! For help') {
        $logB = Wait-Log (Join-Path $nodeBDirectory 'logs\latest.log') 'Done \([0-9.]+s\)! For help' $deadline
    }
    foreach ($nodeProcess in @($nodeA, $nodeB)) {
        $nodeProcess.Process.StandardInput.WriteLine('shardingbase')
        $nodeProcess.Process.StandardInput.WriteLine('version')
        $nodeProcess.Process.StandardInput.WriteLine('stop')
        $nodeProcess.Process.StandardInput.Flush()
    }
    foreach ($nodeProcess in @($nodeA, $nodeB)) {
        if (-not $nodeProcess.Process.WaitForExit(90000)) {
            throw "Backend supervisor $($nodeProcess.Process.Id) did not exit after a normal Minecraft stop"
        }
        if ($nodeProcess.Process.ExitCode -ne 0) {
            throw "Backend supervisor $($nodeProcess.Process.Id) exited with code $($nodeProcess.Process.ExitCode)"
        }
    }
    $proxy.Process.StandardInput.WriteLine('shutdown')
    $proxy.Process.StandardInput.Flush()
    if (-not $proxy.Process.WaitForExit(30000)) {
        throw 'Velocity did not exit after its shutdown command'
    }
    $proxyOutput = $proxy.StandardOutput.GetAwaiter().GetResult() + $proxy.StandardError.GetAwaiter().GetResult()
    if ($proxyOutput -match 'controller failed to initialize|No suitable driver|ExceptionInInitializerError') {
        throw 'Velocity logged a Shardingbase initialization failure'
    }
    if (([regex]::Matches($proxyOutput, 'established a persistent control session')).Count -lt 2) {
        throw 'Velocity did not observe both persistent node sessions'
    }
    if (-not (Test-Path -LiteralPath (Join-Path $proxyDirectory 'plugins\shardingbase\shardingbase.db'))) {
        throw 'Velocity did not create its SQLite authority database'
    }
    $passed = $true
} catch {
    $failure = $_.Exception.Message
} finally {
    foreach ($captured in @($nodeA, $nodeB, $proxy)) {
        if ($null -ne $captured -and -not $captured.Process.HasExited) {
            $captured.Process.Kill($true)
            $captured.Process.WaitForExit()
        }
    }
}

$result = [ordered]@{
    passed = $passed
    runtime = $runDirectory
    velocityVersion = '4.1.1-24'
    backendAEnabled = Test-Path -LiteralPath (Join-Path $nodeADirectory 'logs\latest.log') -PathType Leaf
    backendBEnabled = Test-Path -LiteralPath (Join-Path $nodeBDirectory 'logs\latest.log') -PathType Leaf
    failure = $failure
}
$resultPath = Join-Path $resultsRoot "$runId.json"
$result | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $resultPath -Encoding utf8NoBOM
$result | ConvertTo-Json -Depth 4
if (-not $passed) {
    exit 1
}
