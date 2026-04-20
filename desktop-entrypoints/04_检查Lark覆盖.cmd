@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\check-coverage.ps1"
exit /b %ERRORLEVEL%
