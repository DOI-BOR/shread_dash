@ECHO OFF
TITLE Loading python environment...
set root=C:\Users\%USERNAME%\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\%USERNAME%\AppData\Local\miniforge3\envs\shread
call activate %env%

TITLE Set dates for SHREAD Import...

set /p all="Run last 10 days? (Y/N)"

if %all%==y GOTO ten 
if %all%==Y GOTO ten
if %all%==yes GOTO ten
if %all%==YES GOTO ten
GOTO postten

:ten
	set sdate=10
	set edate=TODAY
	GOTO main
:postten
	set /p sdate="Start Date (YYYYMMDD or # days): "
	set /p edate="End Date (YYYYMMDD or TODAY): "
	
:main
	set shread_dir=C:\Programs\
	cd %shread_dir%shread_wd\
	TITLE Running shread...
	@ECHO ON
	set start=%time%
	mkdir %shread_dir%\shread_dash\database\SHREAD\data
	call python %shread_dir%shread/shread.py -i shread_config_shread_dash.ini -s %sdate% -e %edate% -t D -p snodas
	@ECHO OFF

	TITLE Updating DB with new data...
	set env=C:\Users\%USERNAME%\AppData\Local\miniforge3\envs\shread_env
	call activate %env%
	cd %shread_dir%shread_dash\database\SHREAD\
	@ECHO ON
	python shread_snow_to_db.py
	@ECHO OFF

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
