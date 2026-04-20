@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\force-refresh-8-groups.ps1"
if errorlevel 1 exit /b %ERRORLEVEL%
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\run-weekly.ps1"
exit /b %ERRORLEVEL%
