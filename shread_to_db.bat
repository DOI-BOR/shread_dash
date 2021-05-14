@ECHO OFF
TITLE "Updating SHREAD SNODAS DBs"
set root=C:\ProgramData\Miniconda3
call %root%\Scripts\activate.bat
set env=C:\python_env\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
python C:\python_env\shread_plot\database\SNODAS\shread_to_db.py
pause

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%