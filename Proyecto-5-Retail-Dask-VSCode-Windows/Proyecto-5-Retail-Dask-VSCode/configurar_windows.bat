@echo off
setlocal
cd /d "%~dp0"
echo ==============================================
echo  CONFIGURACION DEL PROYECTO 5 - WINDOWS
 echo ==============================================
where py >nul 2>&1
if errorlevel 1 (
  echo ERROR: No se encontro el lanzador de Python ^(py^).
  echo Instala Python 3.11 de https://www.python.org/downloads/
  pause
  exit /b 1
)
py -3.11 --version >nul 2>&1
if not errorlevel 1 (
  echo Usando Python 3.11
  py -3.11 -m venv .venv
) else (
  py -3.12 --version >nul 2>&1
  if errorlevel 1 (
    echo ERROR: instala Python 3.11 o 3.12 para continuar.
    pause
    exit /b 1
  )
  echo Usando Python 3.12
  py -3.12 -m venv .venv
)
if errorlevel 1 goto fallo
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto fallo
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto fallo
echo.
echo Instalacion terminada. Ejecuta ejecutar_menu.bat
pause
exit /b 0
:fallo
echo.
echo Fallo la preparacion. Revisa la conexion a Internet y los mensajes previos.
pause
exit /b 1
