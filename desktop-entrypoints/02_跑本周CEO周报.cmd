@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\run-weekly.ps1"
exit /b %ERRORLEVEL%
