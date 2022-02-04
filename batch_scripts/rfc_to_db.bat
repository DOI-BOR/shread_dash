@ECHO OFF
TITLE "Refreshing FLOW DBs"
set root=C:\Users\tclarkin\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
set SS=%time%
python C:\Programs\shread_dash\database\FLOW\rfc_to_db.py
set EE=%time%
set /A total=%EE%-%SS%
echo process took %total%

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%

pause