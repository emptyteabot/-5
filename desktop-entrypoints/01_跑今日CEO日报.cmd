@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\run-daily.ps1"
exit /b %ERRORLEVEL%
