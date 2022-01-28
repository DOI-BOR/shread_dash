@ECHO OFF
TITLE "Preparing to update databases"

set /p all="Update ALL (Y/N)? "

if %all%==Y (
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
if %snodas%==Y start batch_scripts\shread_snow_to_db.bat

set /p ndfd="Update NDFD (can take up to 2 hours) (Y/N)? "
if %ndfd%==Y start batch_scripts\shread_ndfd_to_db.bat


set /p csas="Update CSAS (Y/N)? "
if %csas%==Y start batch_scripts\csas_to_db.bat

set /p snotel="Update CSAS Dust (Y/N)? "
if %snotel%==Y start batch_scripts\update_dust.bat

set /p rfc="Update RFC (Y/N)? "
if %rfc%==Y start batch_scripts\rfc_to_db.bat

set /p usgs="Update USGS (Y/N)? "
if %usgs%==Y start batch_scripts\usgs_to_db.bat

set /p snotel="Update SNOTEL (Y/N)? "
if %snotel%==Y start batch_scripts\snotel_to_db.bat

pause
exit