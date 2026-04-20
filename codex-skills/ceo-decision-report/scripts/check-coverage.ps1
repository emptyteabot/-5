$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'resolve-repo-root.ps1')

$RepoRoot = Resolve-BydfiRepoRoot
Set-Location -LiteralPath $RepoRoot

python -X utf8 scripts\check_group_coverage.py --run-discover --stale-hours 168 --text --write-json output\coverage_latest.json
exit $LASTEXITCODE
