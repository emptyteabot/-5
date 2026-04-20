@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ceo-decision-report\scripts\render-final-pdf.ps1"
exit /b %ERRORLEVEL%
