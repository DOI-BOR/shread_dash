@ECHO OFF
TITLE "SHREAD View - port 8081"
set root=C:\ProgramData\Miniconda3
call %root%\Scripts\activate.bat
set env=C:\python_env\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
set PYTHONPATH=%THIS_DIR%
call waitress-serve --port 8081 "shread_dash:app.server"
pause

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%