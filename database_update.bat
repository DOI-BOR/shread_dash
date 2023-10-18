@ECHO OFF
TITLE "Preparing to update databases"

set /p all="Update type? (FULL, RAPID, or CUSTOM) "

if %all%==full GOTO full 
if %all%==Full GOTO full
if %all%==FULL GOTO full
if %all%==f GOTO full
if %all%==F GOTO full
GOTO postfull

:full
	start batch_scripts\shread_snow_to_db.bat
	start batch_scripts\shread_ndfd_to_db.bat
	start batch_scripts\csas_to_db.bat
	start batch_scripts\update_dust.bat
	start batch_scripts\rfc_to_db.bat
	start batch_scripts\usgs_to_db.bat
	start batch_scripts\snotel_to_db.bat
	exit
:postfull

if %all%==rapid GOTO rapid 
if %all%==RAPID GOTO rapid
if %all%==Rapid GOTO rapid
if %all%==R GOTO rapid
if %all%==r GOTO rapid
GOTO postrapid

:rapid
	start batch_scripts\shread_snow_to_db.bat
	start batch_scripts\shread_ndfd_to_db_nosky.bat
	start batch_scripts\csas_to_db.bat
	start batch_scripts\rfc_to_db.bat
	start batch_scripts\usgs_to_db.bat
	start batch_scripts\snotel_to_db.bat
	exit
:postrapid


set /p snodas="Update SNODAS (Y/N)? "
if %snodas%==Y GOTO snodas
if %snodas%==y GOTO snodas
if %snodas%==Yes GOTO snodas
if %snodas%==yes GOTO snodas
if %snodas%==YES GOTO snodas
GOTO postsnodas
:snodas
	start batch_scripts\shread_snow_to_db_dayopt.bat
:postsnodas

set /p ndfd="Update NDFD (can take up to 2 hours) (Y/N)? "
if %ndfd%==Y GOTO ndfd
if %ndfd%==y GOTO ndfd
if %ndfd%==Yes GOTO ndfd
if %ndfd%==yes GOTO ndfd
if %ndfd%==YES GOTO ndfd
GOTO postndfd
:ndfd
	start batch_scripts\shread_ndfd_to_db_skyopt.bat
:postndfd

set /p csas="Update CSAS (Y/N)? "
if %csas%==Y GOTO csas
if %csas%==y GOTO csas
if %csas%==Yes GOTO csas
if %csas%==yes GOTO csas
if %csas%==YES GOTO csas
GOTO postcsas
:csas
	start batch_scripts\csas_to_db.bat
:postcsas

set /p snotel="Update CSAS Dust (Y/N)? "
if %snotel%==Y GOTO dust
if %snotel%==y GOTO dust
if %snotel%==Yes GOTO snotel
if %snotel%==yes GOTO snotel
if %snotel%==YES GOTO snotel
GOTO postdust
:dust
	start batch_scripts\update_dust.bat
:postdust

set /p rfc="Update RFC (Y/N)? "
if %rfc%==Y GOTO rfc
if %rfc%==y GOTO rfc
if %rfc%==Yes GOTO rfc
if %rfc%==yes GOTO rfc
if %rfc%==YES GOTO rfc
GOTO postrfc
:rfc
	start batch_scripts\rfc_to_db.bat
:postrfc

set /p usgs="Update USGS (Y/N)? "
if %usgs%==Y GOTO usgs
if %usgs%==y GOTO usgs
if %usgs%==Yes GOTO usgs
if %usgs%==yes GOTO usgs
if %usgs%==YES GOTO usgs
GOTO postusgs
:usgs
	start batch_scripts\usgs_to_db.bat
:postusgs

set /p snotel="Update SNOTEL (Y/N)? "
if %snotel%==Y GOTO snotel
if %snotel%==y GOTO snotel
if %snotel%==Yes GOTO snotel
if %snotel%==yes GOTO snotel
if %snotel%==YES GOTO snotel
GOTO postsnotel
:snotel
	start batch_scripts\snotel_to_db.bat
:postsnotel

exit