Last Updated: 2022-05-27

README for:
Snow-Hydrology Repo for Evaluation, Analysis, and Decision-making Dashboard (shread_dash.py)

This repository contains a series of batch scripts and python codes to run the Snow-Hydrology Repo for Evaluation, 
Analysis, and Decision-making Dashboard (or "SHREAD Dash"). SHREAD plot has two main components: the database and
the dashboard. The database relies on a series of retrieval scripts (/database/) that retrieve hydrometeorological
data from online and store the data in local databases. Part of the retrieval process is dependent on the SHREAD
repository (https://github.com/usbr/shread), The databases are built in SQLite. The dashboard is built with
Dash by Plotly and is configured to run locally.

This repository is licensed as Creative Commons Zero (CC0). All contributions will be licensed as CC0.

Directions for initial setup and subsequent updates are listed below.


>> INTIAL SETUP <<
Follow these steps if this is your first installation of the dashboard.

1. INSTALL PROGRAMS:
	
	A. Python
		If you don't already have Python, install miniforge: https://github.com/conda-forge/miniforge#download
		You will want to install this LOCALLY. The path should be something like:
			C:\Users\{your user name}\AppData\Local\miniforge3
		If you already have Python installed, be sure to note the directory where it is installed and use that
		for the edits described in Step 4C.
		
	B. GitHub
		If you don't already have an account on GitHub, join here: https://www.github.com/join
		Next, install Git Desktop: https://desktop.github.com/
		and/or Git Bash: https://git-scm.com/download/win 

2. CREATE DIRECTORIES
	
	A. Create directories
		Create a folder "Programs" on your C: drive. Should have path: C:/Programs
		Create a folder "shread_wd" in Programs. Should have path: C:/Programs/shread_wd
		
3. CLONE REPOSITORIES

	The repositories have the following URLs and Local Path locations:

	shread_dash repository:
		URL: https://github.com/usbr/shread_dash
		Local Path: C:/Programs (Should result in path: C:/Programs/shread_dash)
		
	shread repository:
		URL: https://github.com/usbr/shread
		Local Path: C:/Programs (Should result in path: C:/Programs/shread)

	Do one of the following for both repositories:

	A. Using Git Desktop
		Click "File" > "Clone repository" from top bar
		Click the "URL" tab
		Enter the URL and Local Path from above
		Click "Clone"

	B. Using Git Bash
		Navigate to the Local Path above:
			> cd {Local Path}
		Clone repository using the URL above:
			> git clone {URL}

4. BUILD ENVIRONMENTS AND CONFIGURE
	
	A. Build Python Environments
		Open miniforge prompt
		Navigate to shread directory:
			> cd C:/Programs/shread
		Build shread environment:
			> conda env create -f environment.yml
		Navigate to shread_dash directory:
			> cd C:/Programs/shread_dash
		Build shread_env environment:
			> conda env create -f environment.yml
		Close miniforge prompt

	B. Configure shread working directory
		In C:/Programs/shread_wd, add the following folders:
			data/archive
			data/database
			data/working
			resources/gis
		
		To C:Programs/shread_wd, copy shread_config_shread_dash.ini from C:/Programs/shread/config
		To C:/Programs/shread_wd/resources/gis, copy the two .geojson files found in C:/Programs/shread_dash/resources
		
	C. Configure bat files
		Open the following files with a text editor and make sure the path to miniconda is correct.
			C:/Programs/shread_dash/dasboard_deploy.bat (line 3 and 5)
			All files in C:/Programs/shread_dash/batch_scripts/
			
		This can easily be done using Notepad++. Open all files. Hold Ctrl+H.
			In "Find What" enter: C:\Users\tclarkin\AppData\Local\miniforge3
			In "Replace" with enter: C:\Users\{your username}\AppData\Local\miniforge3
			Click "Replace All in All Opened Documents"
			Save All.
			
			**Note that some of the lines will have the addition of \envs\shread or \envs\shread_env

5. RUN

	A. Update databases
		Double click on C:/Programs/shread_dash/database_update.bat
		Follow prompts

	B. Deploy dashboard
		Double click on C:/Programs/shread_dash/dashboard_deploy.bat
		This will open the browser to the dashboard webpage
		If this does not, navigate to http://127.0.0.1:5000/ in browser


>> INTERMEDIATE UPDATES <<
If you have already installed and configured everything for shread plot, but need to pull updates, do the following:

6. PULL UPDATES FROM GITHUB

	The repositories are saved in the following Local Path locations:

	shread_dash repository:
		Local Path: C:/Programs/shread_dash
		
	shread repository:
		Local Path: C:/Programs/shread

	Do one of the following to update the repositories:

	A. Using Git Desktop
		Select the repositories (top left)
		Select "Repository" > "Pull Origin" from top bar

	B. Using Git Bash
		Navigate to the Local Path
			> cd {Local Path}
		Pull the updates: 
			> git pull
	
7. CHECK BAT FILES & ENVIRONMENTS

	A. Check batch scripts
		Repeat Step 4C

	If the updates pulled from GitHub resulted in any changes to the environment.yml files, do the following:

	B. Update Environments (Optional):
		Open miniforge prompt
		Navigate to directory for the repository with changed environment.yml file:
			> cd {Local Directory}
		Update shread environment:
			> conda env update -f environment.yml

>> SCHEDULED DATABASE UPDATES <<
If you would like the database to be updated automatically, do the following:

1. Open the Task Scheduler app on Windows
2. Select "Create Task"
	A. Under General, give the task a name, leave all other settings
	B. Under Triggers, select "New", set to a "Daily" schedule to run at your desired time (recommend 4AM)
	C. Under Actions, select "New", leave action on "Start a program", set Program/script to:
		"C:\Programs\shread_dash\batch_scripts/database_update_full.bat"
	D. Under Conditions, check "Start only if the following network connection is available"
		Leave on "Any connection"
	E. Click "Ok"
3. The task should now run on your desired schedule, as long as you are logged on and connected to the internet


