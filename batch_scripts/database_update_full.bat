@ECHO OFF
TITLE "Preparing to update databases"

start C:\Programs\shread_dash\batch_scripts\shread_snow_to_db.bat
start C:\Programs\shread_dash\batch_scripts\shread_ndfd_to_db.bat
start C:\Programs\shread_dash\batch_scripts\csas_to_db.bat
start C:\Programs\shread_dash\batch_scripts\update_dust.bat
start C:\Programs\shread_dash\batch_scripts\rfc_to_db.bat
start C:\Programs\shread_dash\batch_scripts\usgs_to_db.bat
start C:\Programs\shread_dash\batch_scripts\snotel_to_db.bat
exit
