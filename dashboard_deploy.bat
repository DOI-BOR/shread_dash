@ECHO OFF
TITLE "SHREAD View - port 5000"
set root=C:\Users\tclarkin\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
set PYTHONPATH=%THIS_DIR%
python shread_dash.py

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%
