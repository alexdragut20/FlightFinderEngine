<#
.SYNOPSIS
Restarts the local FlightFinder Engine server on Windows.

.DESCRIPTION
Stops the previously recorded server process, frees the requested port,
optionally cleans orphaned Playwright processes, and starts the server again
using the repository virtual environment when available.

.PARAMETER Port
TCP port to bind for the local web server.

.PARAMETER BindHost
Host interface to bind for the local web server.

.PARAMETER AllowPlaywright
Enables Playwright-backed providers for the restarted server process.

.PARAMETER AssistSkyscanner
Allows Skyscanner to open a visible local browser window so you can complete
anti-bot verification in the normal browsing flow when needed.

.PARAMETER Foreground
Runs the server in the current PowerShell session instead of the background.

.PARAMETER NoKillPlaywright
Skips cleanup of orphaned Playwright and headless browser processes.

.EXAMPLE
.\scripts\restart_server.ps1

.EXAMPLE
.\scripts\restart_server.ps1 -Port 8001 -Foreground

.EXAMPLE
.\scripts\restart_server.ps1 -AllowPlaywright
#>
[CmdletBinding()]
param(
    [int]$Port = 8000,
    [Alias("Host")]
    [string]$BindHost = "127.0.0.1",
    [switch]$AllowPlaywright,
    [switch]$AssistSkyscanner,
    [switch]$Foreground,
    [switch]$NoKillPlaywright
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$LogsDir = Join-Path $ProjectRoot "logs"
$PidFile = Join-Path $LogsDir "server.pid"
$StdOutLog = Join-Path $LogsDir "server.out"
$StdErrLog = Join-Path $LogsDir "server.err"
$AllowPlaywrightValue = if ($AllowPlaywright) { "1" } else { "0" }
$GoogleFlightsFetchModeValue = if ($AllowPlaywright) { "local" } else { "common" }
$SkyscannerPlaywrightFallbackValue = if ($AllowPlaywright) { "1" } else { "0" }
$SkyscannerPlaywrightAssistedValue = if ($AllowPlaywright -and $AssistSkyscanner) { "1" } else { "0" }

function Write-Section {
    param([string]$Title)

    Write-Host ""
    Write-Host "== $Title =="
}

function Set-InheritedEnvironmentValue {
    param(
        [string]$Name,
        [string]$Value
    )

    $previous = [Environment]::GetEnvironmentVariable($Name, "Process")
    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    return $previous
}

function Restore-InheritedEnvironmentValue {
    param(
        [string]$Name,
        [AllowNull()][string]$PreviousValue
    )

    [Environment]::SetEnvironmentVariable($Name, $PreviousValue, "Process")
}

function Resolve-PythonExecutable {
    $venvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        return $venvPython
    }

    $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($null -ne $pythonCommand) {
        return $pythonCommand.Source
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $pythonCommand) {
        return $pythonCommand.Source
    }

    throw "Python executable not found. Create the virtual environment or install Python first."
}

function Stop-ProcessIds {
    param(
        [int[]]$ProcessIds,
        [string]$Label
    )

    $uniqueProcessIds = @($ProcessIds | Where-Object { $_ -gt 0 } | Select-Object -Unique)
    if ($uniqueProcessIds.Count -eq 0) {
        Write-Host "No $Label found."
        return
    }

    Write-Host "Stopping ${Label}: $($uniqueProcessIds -join ', ')"
    foreach ($processId in $uniqueProcessIds) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

function Get-ListeningProcessIds {
    param([int]$TargetPort)

    $connections = @(Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue)
    if ($connections.Count -gt 0) {
        return @($connections | Select-Object -ExpandProperty OwningProcess -Unique)
    }

    $netstatMatches = @(
        netstat -ano -p tcp |
            Select-String -Pattern "^\s*TCP\s+\S+:$TargetPort\s+\S+\s+LISTENING\s+\d+\s*$"
    )
    if ($netstatMatches.Count -eq 0) {
        return @()
    }

    $processIds = foreach ($match in $netstatMatches) {
        $columns = $match.Line -split "\s+"
        if ($columns.Count -gt 0) {
            $columns[-1]
        }
    }
    return @($processIds | Where-Object { $_ -match '^\d+$' } | ForEach-Object { [int]$_ } | Select-Object -Unique)
}

function Get-RepoProcessIds {
    param([string[]]$Patterns)

    $processes = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine.Contains($ProjectRoot)
    }
    $matched = foreach ($process in $processes) {
        foreach ($pattern in $Patterns) {
            if ($process.CommandLine -like "*$pattern*") {
                $process.ProcessId
                break
            }
        }
    }
    return @($matched | Where-Object { $_ } | Select-Object -Unique)
}

function Get-OrphanProcessIds {
    param([string[]]$Patterns)

    $processes = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine
    }
    $matched = foreach ($process in $processes) {
        foreach ($pattern in $Patterns) {
            if ($process.CommandLine -like "*$pattern*") {
                $process.ProcessId
                break
            }
        }
    }
    return @($matched | Where-Object { $_ } | Select-Object -Unique)
}

function Test-ServerReady {
    param(
        [string]$TargetHost,
        [int]$TargetPort
    )

    $serverUrl = "http://${TargetHost}:$TargetPort/api/presets"
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $serverUrl -Method Get -TimeoutSec 2
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500)
    } catch {
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $asyncResult = $client.BeginConnect($TargetHost, $TargetPort, $null, $null)
            $connected = $asyncResult.AsyncWaitHandle.WaitOne(1500)
            if ($connected -and $client.Connected) {
                $client.EndConnect($asyncResult)
                return $true
            }
            return $false
        } finally {
            if ($null -ne $client) {
                $client.Dispose()
            }
        }
    }
}

function Wait-ForServerReady {
    param(
        [string]$TargetHost,
        [int]$TargetPort,
        [int]$TimeoutSeconds
    )

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        if (Test-ServerReady -TargetHost $TargetHost -TargetPort $TargetPort) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

if ($Port -le 0 -or $Port -gt 65535) {
    throw "Invalid port: $Port"
}

Set-Location $ProjectRoot
New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null

$pythonExe = Resolve-PythonExecutable

Write-Host "== FlightFinder Engine restart =="
Write-Host "Project: $ProjectRoot"
Write-Host "Python: $pythonExe"
Write-Host "Host: $BindHost"
Write-Host "Port: $Port"
Write-Host "ALLOW_PLAYWRIGHT_PROVIDERS=$AllowPlaywrightValue"
Write-Host "GOOGLE_FLIGHTS_FETCH_MODE=$GoogleFlightsFetchModeValue"
Write-Host "SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK=$SkyscannerPlaywrightFallbackValue"
Write-Host "SKYSCANNER_PLAYWRIGHT_ASSISTED=$SkyscannerPlaywrightAssistedValue"
Write-Host "Logs: $StdOutLog and $StdErrLog"
Write-Host "PID file: $PidFile"

Write-Section "Stopping any previously recorded server PID"
if (Test-Path $PidFile) {
    $oldPidRaw = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    [int]$oldPid = 0
    if ([int]::TryParse($oldPidRaw, [ref]$oldPid) -and $oldPid -gt 0) {
        Stop-ProcessIds -ProcessIds @($oldPid) -Label "PID from $PidFile"
    } else {
        Write-Host "PID file did not contain a running process id."
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "No PID file found."
}

Write-Section "Stopping any listener on port $Port"
Stop-ProcessIds -ProcessIds (Get-ListeningProcessIds -TargetPort $Port) -Label "listener(s)"

Write-Section "Stopping repo-local server processes"
Stop-ProcessIds -ProcessIds (Get-RepoProcessIds -Patterns @("server.py", "flightfinder-engine")) -Label "repo server process(es)"

if (-not $NoKillPlaywright) {
    Write-Section "Stopping orphaned Playwright processes"
    Stop-ProcessIds -ProcessIds (
        Get-OrphanProcessIds -Patterns @(
            "playwright\driver\node",
            "chrome-headless-shell",
            "ms-playwright"
        )
    ) -Label "Playwright process(es)"
}

Write-Section "Starting server"
$previousHost = Set-InheritedEnvironmentValue -Name "HOST" -Value $BindHost
$previousPort = Set-InheritedEnvironmentValue -Name "PORT" -Value ([string]$Port)
$previousPlaywright = Set-InheritedEnvironmentValue -Name "ALLOW_PLAYWRIGHT_PROVIDERS" -Value $AllowPlaywrightValue
$previousGoogleFlightsFetchMode = Set-InheritedEnvironmentValue -Name "GOOGLE_FLIGHTS_FETCH_MODE" -Value $GoogleFlightsFetchModeValue
$previousSkyscannerPlaywrightFallback = Set-InheritedEnvironmentValue -Name "SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK" -Value $SkyscannerPlaywrightFallbackValue
$previousSkyscannerPlaywrightAssisted = Set-InheritedEnvironmentValue -Name "SKYSCANNER_PLAYWRIGHT_ASSISTED" -Value $SkyscannerPlaywrightAssistedValue
try {
    if ($Foreground) {
        Write-Host "Foreground mode (Ctrl+C to stop)."
        & $pythonExe -u "server.py"
        exit $LASTEXITCODE
    }

    if (Test-Path $StdOutLog) {
        Remove-Item $StdOutLog -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $StdErrLog) {
        Remove-Item $StdErrLog -Force -ErrorAction SilentlyContinue
    }

    $serverProcess = Start-Process `
        -FilePath $pythonExe `
        -ArgumentList "-u", "server.py" `
        -WorkingDirectory $ProjectRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StdOutLog `
        -RedirectStandardError $StdErrLog `
        -PassThru
} finally {
    Restore-InheritedEnvironmentValue -Name "HOST" -PreviousValue $previousHost
    Restore-InheritedEnvironmentValue -Name "PORT" -PreviousValue $previousPort
    Restore-InheritedEnvironmentValue -Name "ALLOW_PLAYWRIGHT_PROVIDERS" -PreviousValue $previousPlaywright
    Restore-InheritedEnvironmentValue -Name "GOOGLE_FLIGHTS_FETCH_MODE" -PreviousValue $previousGoogleFlightsFetchMode
    Restore-InheritedEnvironmentValue -Name "SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK" -PreviousValue $previousSkyscannerPlaywrightFallback
    Restore-InheritedEnvironmentValue -Name "SKYSCANNER_PLAYWRIGHT_ASSISTED" -PreviousValue $previousSkyscannerPlaywrightAssisted
}

if (Wait-ForServerReady -TargetHost $BindHost -TargetPort $Port -TimeoutSeconds 12) {
    $activeProcessIds = @(Get-ListeningProcessIds -TargetPort $Port)
    $serverPid = if ($activeProcessIds.Count -gt 0) { $activeProcessIds[0] } else { $serverProcess.Id }
    Set-Content -Path $PidFile -Value $serverPid -Encoding ascii

    Write-Host "Server started (PID $serverPid)."
    Write-Host "Open: http://$BindHost`:$Port"
    exit 0
}

Write-Error "Server failed to bind to $BindHost`:$Port after 12 seconds."
if (Test-Path $StdOutLog) {
    Write-Host "Last 80 lines of ${StdOutLog}:"
    Get-Content $StdOutLog -Tail 80 -ErrorAction SilentlyContinue
}
if (Test-Path $StdErrLog) {
    Write-Host "Last 80 lines of ${StdErrLog}:"
    Get-Content $StdErrLog -Tail 80 -ErrorAction SilentlyContinue
}
exit 1
