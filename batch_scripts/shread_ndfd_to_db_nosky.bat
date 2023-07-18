@ECHO OFF

TITLE Removing previous files...
set shread_dir=C:\Programs\
mkdir -p %shread_dir%\shread_dash\database\SHREAD\data
cd %shread_dir%\shread_dash\database\SHREAD\data
del ndfd*.tif
del ndfd*.csv
del *.bin
mkdir -p %shread_dir%\shread_wd\data\working\ndfd
cd %shread_dir%\shread_wd\data\working\ndfd
del * /q

TITLE Loading python environment...
set root=C:\Users\tclarkin\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread
call activate %env%

cd %shread_dir%shread_wd\
TITLE Running shread...
set start=%time%

call python %shread_dir%shread/shread.py -i %shread_dir%shread/config/shread_config_shread_dash_nosky.ini -s 20220101 -e 20220101 -t D -p ndfd

TITLE Updating DB with new data...
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
call activate %env%
cd %shread_dir%shread_dash\database\SHREAD\
@ECHO ON
python shread_ndfd_to_db.py
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

ndfd_parameters = mint,maxt,rhm,pop12,qpf,snow,sky