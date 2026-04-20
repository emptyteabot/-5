$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'resolve-repo-root.ps1')

$RepoRoot = Resolve-BydfiRepoRoot
Set-Location -LiteralPath $RepoRoot

$registryPath = Join-Path $RepoRoot 'config\lark_group_registry.json'
$titles = @(
    & python -X utf8 -c "import json,sys; data=json.load(open(sys.argv[1], encoding='utf-8')); [print(item['title']) for item in data.get('groups', []) if item.get('required', True)]" $registryPath
)

if (-not $titles -or $titles.Count -eq 0) {
    throw "No required Lark group titles found in registry: $registryPath"
}

$args = @(
    '-X', 'utf8',
    'scripts\collect_registered_groups.py',
    '--run-discover',
    '--refresh-hours', '24',
    '--skip-summarize',
    '--write-json', 'output\manual_force8_latest.json'
)

foreach ($title in $titles) {
    $args += '--title'
    $args += $title
}

python @args
exit $LASTEXITCODE
