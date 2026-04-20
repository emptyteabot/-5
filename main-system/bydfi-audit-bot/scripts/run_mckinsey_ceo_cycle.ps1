param(
    [ValidateSet("daily", "weekly")]
    [string]$Period = "daily",
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$PythonExe = "python",
    [switch]$RenderFinalPdf,
    [switch]$Deliver,
    [switch]$SkipDiscover,
    [switch]$SkipAutoCollect
)

Set-Location -LiteralPath $RepoRoot

$runnerPath = Join-Path $RepoRoot "run_mckinsey_ceo_cycle.py"
$runnerArgs = @("-X", "utf8", $runnerPath, "--period", $Period)

if ($RenderFinalPdf) {
    $runnerArgs += "--render-final-pdf"
}
if ($Deliver) {
    $runnerArgs += "--deliver"
}
if ($SkipDiscover) {
    $runnerArgs += "--skip-discover"
}
if ($SkipAutoCollect) {
    $runnerArgs += "--skip-auto-collect"
}

& $PythonExe @runnerArgs
exit $LASTEXITCODE
