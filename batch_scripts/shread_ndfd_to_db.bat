@ECHO OFF
set /p all="Update NDFD (can take up to 2 hours) (Y/N)? "

if %all%==Y (
	TITLE Loading python environment...
	set root=C:\Users\tclarkin\AppData\Local\miniforge3
	call %root%\Scripts\activate.bat
	set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread
	call activate %env%

	set shread_dir=C:\Programs\
	cd %shread_dir%shread_wd\
	TITLE Running shread...
	@ECHO ON
	call python %shread_dir%shread/shread.py -i shread_config_example.ini -s 20220101 -e 20220101 -t D -p ndfd
	@ECHO OFF

	TITLE Updating DB with new data...
	set env=C:\Users\tclarkin\AppData\Local\miniforge3\envs\shread_env
	call activate %env%
	cd %shread_dir%shread_plot\database\SHREAD\
	@ECHO ON
	python shread_ndfd_to_db.py
	@ECHO OFF

	pause

	:GET_THIS_DIR
	set THIS_DIR=%~dp0
	pushd %THIS_DIR%
	
)