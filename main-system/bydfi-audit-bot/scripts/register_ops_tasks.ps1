param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$PythonExe = "python",
    [string]$DailyTime = "08:00",
    [string]$WeeklyTime = "09:00"
)

$startScript = Join-Path $RepoRoot "scripts\start_ops_cycle.ps1"
$dailyTaskName = "BYDFI-Daily-Ops-Cycle"
$weeklyTaskName = "BYDFI-Weekly-Ops-Cycle"

$dailyAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$startScript`" -RepoRoot `"$RepoRoot`" -PythonExe `"$PythonExe`" -Runner daily"
$weeklyAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$startScript`" -RepoRoot `"$RepoRoot`" -PythonExe `"$PythonExe`" -Runner weekly"

$dailyTrigger = New-ScheduledTaskTrigger -Daily -At ([datetime]::ParseExact($DailyTime, "HH:mm", $null))
$weeklyTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At ([datetime]::ParseExact($WeeklyTime, "HH:mm", $null))
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $dailyTaskName -Action $dailyAction -Trigger $dailyTrigger -Settings $settings -Force | Out-Null
Register-ScheduledTask -TaskName $weeklyTaskName -Action $weeklyAction -Trigger $weeklyTrigger -Settings $settings -Force | Out-Null

Write-Output "registered=$dailyTaskName at $DailyTime"
Write-Output "registered=$weeklyTaskName at Sunday $WeeklyTime"
