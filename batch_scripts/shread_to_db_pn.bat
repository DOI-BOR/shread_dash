@ECHO OFF
TITLE "Updating SHREAD SNODAS DBs"
rem set root=C:\ProgramData\Miniconda3
set root=C:\Users\tclarkin\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
rem set env=C:\python_env\shread_env
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
call activate %env%
call :GET_THIS_DIR
call chdir %THIS_DIR%
rem python C:\python_env\shread_plot\database\SNODAS\shread_to_db.py
python C:\Programs\shread_plot\database\SNODAS\shread_to_db.py
pause

:GET_THIS_DIR
set THIS_DIR=%~dp0
pushd %THIS_DIR%