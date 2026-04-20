@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\install-to-codex.ps1"
exit /b %ERRORLEVEL%
