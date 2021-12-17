@ECHO OFF
TITLE Loading python environment...
set root=C:\Users\tclarkin\AppData\Local\miniforge3
call %root%\Scripts\activate.bat
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread
call activate %env%

TITLE Set dates for SHREAD Import...
set /p sdate="Start Date (YYYYMMDD): "
set /p edate="End Date (YYYYMMDD): "
set shread_dir=C:\Programs\
cd %shread_dir%shread_wd\
TITLE Running shread...
@ECHO ON
call python %shread_dir%shread/shread.py -i shread_config_example.ini -s %sdate% -e %edate% -t D -p snodas
@ECHO OFF

TITLE Updating DB with new data...
set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
call activate %env%
cd %shread_dir%shread_plot\database\SHREAD\
@ECHO ON
python shread_to_db.py
@ECHO OFF

TITLE Removing files...
del shread_data\*

exit