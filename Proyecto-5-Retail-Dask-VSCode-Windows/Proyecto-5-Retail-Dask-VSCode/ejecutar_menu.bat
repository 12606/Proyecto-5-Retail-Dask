@echo off
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Primero ejecuta configurar_windows.bat
  pause
  exit /b 1
)
".venv\Scripts\python.exe" -u menu.py
if errorlevel 1 pause
