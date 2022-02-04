@ECHO OFF
TITLE "Updating CSAS dust"
set root=C:\Users\tclarkin\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
python C:\Programs\shread_dash\database\CSAS\csas_dust_update.py

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%

pause