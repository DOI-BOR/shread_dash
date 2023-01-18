@ECHO OFF
TITLE "Preparing to update databases"

set /p all="Update ALL (Y/N)? "

if %all%==Y or %all%==y(
	start batch_scripts\shread_snow_to_db.bat
	start batch_scripts\shread_ndfd_to_db.bat
	start batch_scripts\csas_to_db.bat
	start batch_scripts\update_dust.bat
	start batch_scripts\rfc_to_db.bat
	start batch_scripts\usgs_to_db.bat
	start batch_scripts\snotel_to_db.bat
	pause
	exit
)

set /p snodas="Update SNODAS (Y/N)? "
if %snodas%==Y or %snodas%==y start batch_scripts\shread_snow_to_db.bat

set /p ndfd="Update NDFD (can take up to 2 hours) (Y/N)? "
if %ndfd%==Y or %ndfd%==y start batch_scripts\shread_ndfd_to_db.bat


set /p csas="Update CSAS (Y/N)? "
if %csas%==Y or %csas%==y start batch_scripts\csas_to_db.bat

set /p snotel="Update CSAS Dust (Y/N)? "
if %snotel%==Y or %snotel%==y start batch_scripts\update_dust.bat

set /p rfc="Update RFC (Y/N)? "
if %rfc%==Y or %rfc%==y start batch_scripts\rfc_to_db.bat

set /p usgs="Update USGS (Y/N)? "
if %usgs%==Y or %usgs%==y start batch_scripts\usgs_to_db.bat

set /p snotel="Update SNOTEL (Y/N)? "
if %snotel%==Y or %snotel%==y start batch_scripts\snotel_to_db.bat

pause
exit