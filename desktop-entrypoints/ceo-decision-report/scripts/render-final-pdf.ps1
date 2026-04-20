$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'resolve-repo-root.ps1')

$RepoRoot = Resolve-BydfiRepoRoot
$DesktopRoot = [Environment]::GetFolderPath('Desktop')
$DesktopTarget = Join-Path $DesktopRoot 'CEO_report_final.pdf'
$SourcePath = Join-Path $RepoRoot 'data\reports\ceo_brief_final_send.md'
$OutputPath = Join-Path $RepoRoot 'data\reports\ceo_brief_final_send.pdf'

if (-not (Test-Path -LiteralPath $SourcePath)) {
    throw "Final markdown not found: $SourcePath"
}

Set-Location -LiteralPath $RepoRoot

$args = @(
    '-X', 'utf8',
    'generate_ceo_brief_pdf.py',
    '--source', $SourcePath,
    '--output', $OutputPath,
    '--desktop-target', $DesktopTarget
)

python @args
exit $LASTEXITCODE
