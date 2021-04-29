@ECHO OFF
TITLE "SHREAD View - port 448"
set root=C:\ProgramData\Miniconda3
call %root%\Scripts\activate.bat
set env=shread_env
call activate %env%
cmd /k

:GET_THIS_DIR
pushd %~dp0
set THIS_DIR=%CD%
popd