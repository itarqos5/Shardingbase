[CmdletBinding()]
param(
    [ValidateSet('core', 'worldedit', 'fawe')]
    [string[]] $Profile = @('core', 'worldedit', 'fawe'),
    [string] $ServerJar = (Join-Path $PSScriptRoot '..\build\release\server.jar'),
    [int] $StartupTimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
$testsRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$cacheRoot = Join-Path $testsRoot 'cache'
$runtimeRoot = Join-Path $testsRoot 'runtime'
$resultsRoot = Join-Path $testsRoot 'results'
$minecraftServerCache = Join-Path $cacheRoot 'mojang_26.2.jar'
$serverJarPath = [System.IO.Path]::GetFullPath($ServerJar)
$java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\java.exe' } else { $null }
if (-not $java -or -not (Test-Path -LiteralPath $java -PathType Leaf)) {
    $java = (Get-Command java.exe -ErrorAction Stop).Source
}
if (-not (Test-Path -LiteralPath $serverJarPath -PathType Leaf)) {
    throw "Release server JAR not found: $serverJarPath"
}

New-Item -ItemType Directory -Force -Path $cacheRoot, $runtimeRoot, $resultsRoot | Out-Null
$manifest = Get-Content -Raw -LiteralPath (Join-Path $testsRoot 'plugin-matrix.json') | ConvertFrom-Json

function Get-VerifiedPlugin([object] $plugin) {
    $target = Join-Path $cacheRoot $plugin.file
    if (-not (Test-Path -LiteralPath $target -PathType Leaf) -or
        (Get-FileHash -LiteralPath $target -Algorithm SHA512).Hash.ToLowerInvariant() -ne $plugin.sha512) {
        Write-Host "Downloading $($plugin.name) $($plugin.file)"
        Invoke-WebRequest -Uri $plugin.url -OutFile $target -Headers @{
            'User-Agent' = 'Shardingbase-compat-test/1.0'
        }
    }
    $actual = (Get-FileHash -LiteralPath $target -Algorithm SHA512).Hash.ToLowerInvariant()
    if ($actual -ne $plugin.sha512) {
        throw "SHA-512 mismatch for $($plugin.name): expected $($plugin.sha512), got $actual"
    }
    return $target
}

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        return ([System.Net.IPEndPoint] $listener.LocalEndpoint).Port
    } finally {
        $listener.Stop()
    }
}

$summary = @()
foreach ($profileName in $Profile) {
    $runId = '{0}-{1}-{2}' -f $profileName, (Get-Date -Format 'yyyyMMdd-HHmmss'), ([guid]::NewGuid().ToString('N').Substring(0, 8))
    $runDirectory = [System.IO.Path]::GetFullPath((Join-Path $runtimeRoot $runId))
    if (-not $runDirectory.StartsWith([System.IO.Path]::GetFullPath($runtimeRoot), [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing unsafe runtime path: $runDirectory"
    }
    $pluginsDirectory = Join-Path $runDirectory 'plugins'
    New-Item -ItemType Directory -Force -Path $pluginsDirectory | Out-Null
    Copy-Item -LiteralPath $serverJarPath -Destination (Join-Path $runDirectory 'server.jar')
    Set-Content -LiteralPath (Join-Path $runDirectory 'eula.txt') -Value 'eula=true' -Encoding utf8NoBOM
    $port = Get-FreeTcpPort
    @(
        'online-mode=false'
        'server-ip=127.0.0.1'
        "server-port=$port"
        'level-name=world'
        'spawn-protection=0'
        'view-distance=3'
        'simulation-distance=3'
        'motd=Shardingbase plugin compatibility test'
    ) | Set-Content -LiteralPath (Join-Path $runDirectory 'server.properties') -Encoding ascii

    $selected = @($manifest.plugins | Where-Object { $_.profiles -contains $profileName })
    foreach ($plugin in $selected) {
        Copy-Item -LiteralPath (Get-VerifiedPlugin $plugin) -Destination (Join-Path $pluginsDirectory $plugin.file)
    }

    if (Test-Path -LiteralPath $minecraftServerCache -PathType Leaf) {
        $cachedServer = Get-Item -LiteralPath $minecraftServerCache
        if ($cachedServer.Length -gt 0) {
            $runtimeCache = Join-Path $runDirectory 'cache'
            New-Item -ItemType Directory -Force -Path $runtimeCache | Out-Null
            Copy-Item -LiteralPath $minecraftServerCache -Destination (Join-Path $runtimeCache 'mojang_26.2.jar')
        }
    }

    Write-Host "Starting profile $profileName with $($selected.Count) plugins in $runDirectory"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $java
    $startInfo.WorkingDirectory = $runDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardInput = $true
    foreach ($argument in @('-Xms128M', '-Xmx768M', '-Dterminal.jline=false', '-Dterminal.ansi=false', '-jar', 'server.jar', 'nogui')) {
        [void] $startInfo.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    [void] $process.Start()

    $latestLog = Join-Path $runDirectory 'logs\latest.log'
    $deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
    $started = $false
    while (-not $process.HasExited -and [DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 500
        if (Test-Path -LiteralPath $latestLog) {
            $tail = Get-Content -Raw -LiteralPath $latestLog
            if ($tail -match 'Done \([0-9.]+s\)! For help') {
                $started = $true
                break
            }
        }
    }

    if ($started) {
        foreach ($command in @('version', 'plugins', 'huskhomes status')) {
            $process.StandardInput.WriteLine($command)
            $process.StandardInput.Flush()
            Start-Sleep -Seconds 2
        }
        $process.StandardInput.WriteLine('stop')
        $process.StandardInput.Flush()
    }
    if (-not $process.WaitForExit(90000)) {
        $process.Kill($true)
        $process.WaitForExit()
    }

    $runtimeMinecraftServer = Join-Path $runDirectory 'cache\mojang_26.2.jar'
    if ($started -and $process.ExitCode -eq 0 -and
        (Test-Path -LiteralPath $runtimeMinecraftServer -PathType Leaf) -and
        (Get-Item -LiteralPath $runtimeMinecraftServer).Length -gt 0) {
        Copy-Item -LiteralPath $runtimeMinecraftServer -Destination $minecraftServerCache -Force
    }

    $log = if (Test-Path -LiteralPath $latestLog) { Get-Content -Raw -LiteralPath $latestLog } else { '' }
    $startupLog = if ($log -match '(?s)^(.*?)Done \([0-9.]+s\)! For help') { $Matches[1] } else { $log }
    $failurePatterns = @(
        '\[.*ERROR\]',
        'Could not load .+ in folder plugins',
        'Could not pass event',
        'NoClassDefFoundError',
        'ExceptionInInitializerError',
        'does not fully support your version of Bukkit',
        'Failed to load the built-in legacy id registry',
        'Failed to initialize'
    )
    $findings = @()
    foreach ($pattern in $failurePatterns) {
        $findings += [regex]::Matches($log, "(?im)^.*$pattern.*$") | ForEach-Object Value
    }
    $findings = @($findings | Where-Object {
        $_ -notmatch 'CrashReport preload thread/ERROR.*HkeyPerformanceDataUtil'
    } | Select-Object -Unique)
    $missing = @($selected | Where-Object {
        $logName = if ($_.PSObject.Properties.Name -contains 'logName') { $_.logName } else { $_.name }
        $log -notmatch "(?im)\[$([regex]::Escape($logName))\].*(Enabling|Successfully enabled|Enabled Version|Version .+ Enabled)"
    } | ForEach-Object name)
    $disabled = @($selected | Where-Object {
        $logName = if ($_.PSObject.Properties.Name -contains 'logName') { $_.logName } else { $_.name }
        $startupLog -match "(?im)\[$([regex]::Escape($logName))\].*Disabling"
    } | ForEach-Object name)
    $expectedUnsupported = @($selected | Where-Object {
        $_.PSObject.Properties.Name -contains 'expectedUnsupported' -and $_.expectedUnsupported
    } | ForEach-Object name)
    $unexpectedDisabled = @($disabled | Where-Object { $expectedUnsupported -notcontains $_ })
    $unexpectedMissing = @($missing | Where-Object { $expectedUnsupported -notcontains $_ })
    $result = [ordered]@{
        profile = $profileName
        runtime = $runDirectory
        started = $started
        exitCode = $process.ExitCode
        plugins = @($selected.name)
        missingEnableEvidence = $missing
        disabledDuringStartup = $disabled
        expectedUnsupported = $expectedUnsupported
        unexpectedDisabled = $unexpectedDisabled
        unexpectedMissingEnableEvidence = $unexpectedMissing
        findings = $findings
    }
    $resultPath = Join-Path $resultsRoot "$runId.json"
    $result | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $resultPath -Encoding utf8NoBOM
    $summary += [pscustomobject] $result
    Write-Host "Profile $profileName complete: started=$started exit=$($process.ExitCode) findings=$($result.findings.Count)"
}

$summary | ConvertTo-Json -Depth 6
if ($summary | Where-Object {
    -not $_.started -or $_.exitCode -ne 0 -or $_.findings.Count -ne 0 -or
    $_.unexpectedDisabled.Count -ne 0 -or $_.unexpectedMissingEnableEvidence.Count -ne 0
}) {
    exit 1
}
