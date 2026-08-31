[CmdletBinding()]
param(
    [string] $ServerJar = (Join-Path $PSScriptRoot '..\build\release\server.jar'),
    [int] $StartupTimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
$testsRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$runtimeRoot = Join-Path $testsRoot 'runtime'
$serverJarPath = [System.IO.Path]::GetFullPath($ServerJar)
$java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\java.exe' } else { $null }
if (-not $java -or -not (Test-Path -LiteralPath $java -PathType Leaf)) {
    $java = (Get-Command java.exe -ErrorAction Stop).Source
}
if (-not (Test-Path -LiteralPath $serverJarPath -PathType Leaf)) {
    throw "Release server JAR not found: $serverJarPath"
}

New-Item -ItemType Directory -Force -Path $runtimeRoot | Out-Null
$runId = 'boundary-{0}-{1}' -f (Get-Date -Format 'yyyyMMdd-HHmmss'), ([guid]::NewGuid().ToString('N').Substring(0, 8))
$runDirectory = [System.IO.Path]::GetFullPath((Join-Path $runtimeRoot $runId))
if (-not $runDirectory.StartsWith([System.IO.Path]::GetFullPath($runtimeRoot), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing unsafe runtime path: $runDirectory"
}
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
Copy-Item -LiteralPath $serverJarPath -Destination (Join-Path $runDirectory 'server.jar')
New-Item -ItemType Directory -Force -Path (Join-Path $runDirectory 'plugins') | Out-Null
$observerJar = Join-Path $testsRoot '..\compatibility-fixtures\bukkit\build\libs\Shardingbase-Fixture-Bukkit.jar'
if (-not (Test-Path -LiteralPath $observerJar -PathType Leaf)) {
    throw "Boundary observer fixture not found; run buildShardingbaseCompatibilityFixtures first: $observerJar"
}
Copy-Item -LiteralPath $observerJar -Destination (Join-Path $runDirectory 'plugins\Shardingbase-Fixture-Bukkit.jar')
Set-Content -LiteralPath (Join-Path $runDirectory 'eula.txt') -Value 'eula=true' -Encoding utf8NoBOM

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        return ([System.Net.IPEndPoint] $listener.LocalEndpoint).Port
    } finally {
        $listener.Stop()
    }
}

$port = Get-FreeTcpPort
@(
    'online-mode=false'
    'server-ip=127.0.0.1'
    "server-port=$port"
    'level-name=world'
    'spawn-protection=0'
    'view-distance=2'
    'simulation-distance=2'
    'motd=Shardingbase boundary persistence test'
) | Set-Content -LiteralPath (Join-Path $runDirectory 'server.properties') -Encoding ascii

function Start-TestServer {
    $latestLog = Join-Path $runDirectory 'logs\latest.log'
    if (Test-Path -LiteralPath $latestLog -PathType Leaf) {
        Remove-Item -LiteralPath $latestLog
    }
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $java
    $startInfo.WorkingDirectory = $runDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardInput = $true
    foreach ($argument in @(
        '-Xms128M',
        '-Xmx768M',
        '-Dterminal.jline=false',
        '-Dterminal.ansi=false',
        '-Dshardingbase.boundary-test.cut-chunk=1000',
        '-jar',
        'server.jar',
        'nogui'
    )) {
        [void] $startInfo.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    [void] $process.Start()

    $deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
    while (-not $process.HasExited -and [DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 500
        if ((Test-Path -LiteralPath $latestLog) -and (Get-Content -Raw -LiteralPath $latestLog) -match 'Done \([0-9.]+s\)! For help') {
            return $process
        }
    }
    if (-not $process.HasExited) {
        $process.Kill($true)
        $process.WaitForExit()
    }
    throw "Boundary test server did not start; inspect $latestLog"
}

function Stop-TestServer([System.Diagnostics.Process] $process) {
    $process.StandardInput.WriteLine('stop')
    $process.StandardInput.Flush()
    if (-not $process.WaitForExit(90000)) {
        $process.Kill($true)
        $process.WaitForExit()
        throw 'Boundary test server did not stop cleanly'
    }
    if ($process.ExitCode -ne 0) {
        throw "Boundary test server exited with code $($process.ExitCode)"
    }
}

function Get-PersistedPeerChunks([string] $directory, [int] $cutChunk) {
    $found = [System.Collections.Generic.List[string]]::new()
    foreach ($kind in @('region', 'poi', 'entities')) {
        $kindDirectory = Join-Path $directory $kind
        if (-not (Test-Path -LiteralPath $kindDirectory -PathType Container)) {
            continue
        }
        foreach ($file in Get-ChildItem -LiteralPath $kindDirectory -Filter 'r.*.*.mca' -File) {
            if ($file.BaseName -notmatch '^r\.(-?\d+)\.(-?\d+)$') {
                continue
            }
            $regionX = [int] $Matches[1]
            $regionZ = [int] $Matches[2]
            $stream = [System.IO.File]::Open($file.FullName, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
            try {
                $header = [byte[]]::new(4096)
                if ($stream.Read($header, 0, $header.Length) -ne $header.Length) {
                    throw "Truncated region header: $($file.FullName)"
                }
                for ($index = 0; $index -lt 1024; $index++) {
                    $offset = $index * 4
                    if (($header[$offset] -bor $header[$offset + 1] -bor $header[$offset + 2] -bor $header[$offset + 3]) -eq 0) {
                        continue
                    }
                    $chunkX = $regionX * 32 + ($index % 32)
                    $chunkZ = $regionZ * 32 + [math]::Floor($index / 32)
                    if ($chunkX -ge $cutChunk) {
                        $found.Add("$kind[$chunkX,$chunkZ]")
                    }
                }
            } finally {
                $stream.Dispose()
            }
        }
    }
    return $found
}

Write-Host "Creating baseline world in $runDirectory"
$firstRun = Start-TestServer
Stop-TestServer $firstRun

$worldDirectory = Join-Path $runDirectory 'world'
$overworldDirectory = Join-Path $worldDirectory 'dimensions\minecraft\overworld'
$observations = Join-Path $runDirectory 'plugins\ShardingbaseFixtureBukkit\boundary-observations.txt'
$worldLine = Get-Content -LiteralPath $observations | Where-Object { $_ -match '^world=minecraft:overworld uuid=' } | Select-Object -First 1
if ($worldLine -notmatch 'uuid=([0-9a-f-]{36})$') {
    throw "Boundary observer did not report the overworld UUID: $worldLine"
}
$worldId = $Matches[1]
$cutChunk = 1000
@"
format-version=1
world-key=minecraft:overworld
world-id=$worldId
transaction-id=$([guid]::NewGuid())
axis=X
cut-chunk=$cutChunk
owned-side=NEGATIVE
peer-id=boundary-test-peer
"@ | Set-Content -LiteralPath (Join-Path $overworldDirectory 'shardingbase-shard.properties') -Encoding ascii

Write-Host 'Generating the last locally owned chunk beside an absent peer-owned world'
$secondRun = Start-TestServer
$secondRun.StandardInput.WriteLine('forceload add 15984 0')
$secondRun.StandardInput.Flush()
Start-Sleep -Seconds 15
Stop-TestServer $secondRun

$peerChunks = @(Get-PersistedPeerChunks $overworldDirectory $cutChunk)
if ($peerChunks.Count -ne 0) {
    throw "Peer-owned chunk data was persisted: $($peerChunks -join ', ')"
}

$peerCallbacks = @(Get-Content -LiteralPath $observations | Where-Object {
    $_ -match '^(chunk-load|chunk-populate|block-populator) '
})
if ($peerCallbacks.Count -ne 0) {
    throw "Peer-owned Bukkit lifecycle callbacks ran: $($peerCallbacks -join ', ')"
}

$log = Get-Content -Raw -LiteralPath (Join-Path $runDirectory 'logs\latest.log')
$unexpectedErrors = @([regex]::Matches($log, '(?im)^.*\[.*ERROR\].*$') | ForEach-Object Value | Where-Object {
    $_ -notmatch 'HkeyPerformanceDataUtil'
})
if ($unexpectedErrors.Count -ne 0) {
    throw "Boundary persistence run logged an error; inspect $runDirectory\logs\latest.log"
}
Write-Host "Boundary isolation test passed: no lifecycle callbacks or chunk, POI, or entity data at X >= $cutChunk"
