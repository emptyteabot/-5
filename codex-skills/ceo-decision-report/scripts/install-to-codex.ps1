$ErrorActionPreference = 'Stop'
$SkillRoot = Split-Path -Parent $PSScriptRoot
$TargetRoot = Join-Path $HOME '.codex\skills'
$Target = Join-Path $TargetRoot 'ceo-decision-report'

New-Item -ItemType Directory -Force -Path $TargetRoot | Out-Null
if (-not (Test-Path -LiteralPath $Target)) {
    New-Item -ItemType Directory -Force -Path $Target | Out-Null
}

Copy-Item -Path (Join-Path $SkillRoot '*') -Destination $Target -Recurse -Force
Write-Output "Installed skill to $Target"
