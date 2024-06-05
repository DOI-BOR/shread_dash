@ECHO OFF
TITLE "SHREAD View - port 5001"
set root=C:\Users\%USERNAME%\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\%USERNAME%\AppData\Local\miniforge3\envs\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
set PYTHONPATH=%THIS_DIR%

start "" http://127.0.0.1:5001/

python shread_dash.py

pause

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%
