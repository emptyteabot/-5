$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'resolve-repo-root.ps1')

$RepoRoot = Resolve-BydfiRepoRoot
$Runner = Join-Path $RepoRoot 'scripts\run_mckinsey_ceo_cycle.ps1'

if (-not (Test-Path -LiteralPath $Runner)) {
    throw "Runner not found: $Runner"
}

powershell -NoProfile -ExecutionPolicy Bypass -File $Runner -Period weekly
exit $LASTEXITCODE
