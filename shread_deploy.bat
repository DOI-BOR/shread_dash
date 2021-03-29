@ECHO OFF
TITLE "SHREAD View - port 448"
call :GET_THIS_DIR
call chdir %THIS_DIR%
call cd ..\
set root=.\hdb_env
call %root%\Scripts\activate.bat
call chdir %THIS_DIR%
waitress-serve --port 448 shread_plot:app.server
pause

:GET_THIS_DIR
pushd %~dp0
set THIS_DIR=%CD%
popd