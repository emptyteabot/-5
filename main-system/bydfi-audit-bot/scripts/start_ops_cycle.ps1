param(
    [ValidateSet("daily", "weekly")]
    [string]$Runner = "daily",
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$PythonExe = "python",
    [switch]$RenderCeo,
    [switch]$SkipDigestPdf,
    [switch]$SkipDelivery,
    [switch]$SkipDiscover,
    [switch]$SkipAutoCollect
)

Set-Location -LiteralPath $RepoRoot

$requiredEnvNames = @(
    "BYDFI_TENCENT_HOST",
    "BYDFI_TENCENT_USER",
    "BYDFI_TENCENT_PASSWORD",
    "BYDFI_REPORT_REMOTE_DIR",
    "BYDFI_REPORT_BASE_URL",
    "BYDFI_LARK_WEBHOOK_URL",
    "BYDFI_EXTERNAL_COLLECTOR_PATH",
    "BYDFI_LARK_STORAGE_STATE_PATH"
)

foreach ($envName in $requiredEnvNames) {
    $currentValue = [Environment]::GetEnvironmentVariable($envName, "Process")
    if ([string]::IsNullOrWhiteSpace($currentValue)) {
        $userValue = [Environment]::GetEnvironmentVariable($envName, "User")
        if (-not [string]::IsNullOrWhiteSpace($userValue)) {
            [Environment]::SetEnvironmentVariable($envName, $userValue, "Process")
        }
    }
}

$logsDir = Join-Path $RepoRoot "output\logs"
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logsDir "$Runner`_ops_cycle_$stamp.log"

$runnerPath = if ($Runner -eq "daily") {
    Join-Path $RepoRoot "run_daily_ops_cycle.py"
} else {
    Join-Path $RepoRoot "run_weekly_ops_cycle.py"
}

$runnerArgs = @("-X", "utf8", $runnerPath)

$effectiveSkipDiscover = $SkipDiscover
if (-not $effectiveSkipDiscover -and $Runner -eq "daily") {
    $effectiveSkipDiscover = $true
}

if ($effectiveSkipDiscover) {
    $runnerArgs += "--skip-discover"
}
if ($SkipAutoCollect) {
    $runnerArgs += "--skip-auto-collect"
}
if (-not $SkipDigestPdf) {
    $runnerArgs += "--render-digest-pdf"
}
if (-not $SkipDelivery) {
    $runnerArgs += "--deliver"
}
if ($RenderCeo) {
    $runnerArgs += "--render-ceo"
}

& $PythonExe @runnerArgs 2>&1 | Tee-Object -FilePath $logPath
$exitCode = $LASTEXITCODE
"exit_code=$exitCode" | Add-Content -Path $logPath -Encoding UTF8
exit $exitCode
