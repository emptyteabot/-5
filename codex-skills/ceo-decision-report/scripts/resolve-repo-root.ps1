$ErrorActionPreference = 'Stop'

function Resolve-BydfiRepoRoot {
    $desktop = [Environment]::GetFolderPath('Desktop')
    $candidates = New-Object System.Collections.Generic.List[string]

    $direct = Join-Path $desktop 'bydfi-audit-bot'
    if (-not $candidates.Contains($direct)) {
        $null = $candidates.Add($direct)
    }

    Get-ChildItem -LiteralPath $desktop -Directory -ErrorAction SilentlyContinue | ForEach-Object {
        $candidate = Join-Path $_.FullName 'bydfi-audit-bot'
        if (-not $candidates.Contains($candidate)) {
            $null = $candidates.Add($candidate)
        }
    }

    $ranked = @()
    foreach ($candidate in $candidates) {
        if (-not (Test-Path -LiteralPath $candidate)) {
            continue
        }

        $runnerPath = Join-Path $candidate 'run_mckinsey_ceo_cycle.py'
        $registryPath = Join-Path $candidate 'config\lark_group_registry.json'
        $dbPath = Join-Path $candidate 'data\audit_records.sqlite3'
        $finalMdPath = Join-Path $candidate 'data\reports\ceo_brief_final_send.md'

        if (-not (Test-Path -LiteralPath $runnerPath) -or -not (Test-Path -LiteralPath $registryPath)) {
            continue
        }

        $dbSize = 0
        if (Test-Path -LiteralPath $dbPath) {
            $dbSize = [int64](Get-Item -LiteralPath $dbPath).Length
        }

        $ranked += [pscustomobject]@{
            Path = $candidate
            DbSize = $dbSize
            HasFinalMd = [bool](Test-Path -LiteralPath $finalMdPath)
        }
    }

    $best = $ranked |
        Sort-Object @{ Expression = 'DbSize'; Descending = $true }, @{ Expression = 'HasFinalMd'; Descending = $true } |
        Select-Object -First 1

    if ($null -ne $best -and $best.DbSize -gt 0) {
        return $best.Path
    }

    if ($null -ne $best) {
        return $best.Path
    }

    throw "Could not locate a usable bydfi-audit-bot under Desktop."
}
