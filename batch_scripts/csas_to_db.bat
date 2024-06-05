@ECHO OFF
TITLE "Updating CSAS DBs"
set root=C:\Users\%USERNAME%\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\%USERNAME%\AppData\Local\miniforge3\envs\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
set start=%time%
python C:\Programs\shread_dash\database\CSAS\csas_to_db.py

if %ERRORLEVEL%==0 GOTO success
GOTO fail

:fail
	echo "Update error...please rerun"
	pause
:success
	echo process began at %start%
	echo process complete at %time%
	exit

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%
