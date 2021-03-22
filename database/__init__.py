# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 11:55:27 2021

@author: buriona
"""

import os
import datetime as dt
from pathlib import Path
import pandas as pd
import hydroimport as hydro
import seaborn as sns
from flask_sqlalchemy import SQLAlchemy
import dash_bootstrap_components as dbc
import dash

this_dir = os.path.dirname(os.path.realpath(__file__))
app_dir = os.path.dirname(this_dir)

def create_app():
    assets_path = Path(app_dir, 'assets')
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        update_title="Updating...",
        # suppress_callback_exceptions=True,
        assets_folder=assets_path
    )
    app.title="WCAO Dashboard"
    db_path = Path(app_dir, 'database')
    snodas_all_db_path = Path(db_path, 'snodas.db')
    snodas_swe_db_path = Path(db_path, 'snodas_swe.db')
    snodas_sd_db_path = Path(db_path, 'snodas_sd.db')
    snodas_all_db_con_str = f'sqlite:///{snodas_all_db_path.as_posix()}'
    snodas_swe_db_con_str = f'sqlite:///{snodas_swe_db_path.as_posix()}'
    snodas_sd_db_con_str = f'sqlite:///{snodas_sd_db_path.as_posix()}'
    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.server.config['SQLALCHEMY_DATABASE_URI'] = snodas_all_db_con_str
    app.server.config['SQLALCHEMY_BINDS'] = {
        'snodas_swe': snodas_swe_db_con_str,
        'snodas_sd': snodas_sd_db_con_str
    }

    return app

app = create_app()
db = SQLAlchemy(app.server)
db.reflect()

# Define Functinos
def db_import(data_file):
    """
    # Function to import csv db files
    :param data_file: the file path
    :return: df, database in a date indexed dataframe
    """
    db = pd.read_csv(data_file)
    db.index = pd.to_datetime(db.Date)
    db = db.tz_localize("UTC")

    return (db)


# Function to import csv db files
def csas_db_import(data_file, dtype, db_dir=this_dir):
    """
    # Function to import CSAS db data
    :param data_file: the file path
    :param type: :dv: or :iv:, time step
    :return: df, database in a date indexed dataframe
    """
    data_path = Path(db_dir, data_file)
    db = pd.read_csv(data_path)
    if dtype == "24hr":
        dates = hydro.compose_date(years=db.Year,days=db.DOY)
    if dtype == "1hr":
        dates = hydro.compose_date(years=db.Year, days=db.DOY,hours=db.Hour/100)

    db.index = dates
    db = db.tz_localize("UTC")

    return (db)

### Begin User Input Data
# Define working (data) directory
os.chdir(os.path.join(app_dir, 'database'))

# Identify files in database
data_files = os.listdir(os.path.join(app_dir, 'database'))
res_dir = os.path.join(app_dir, 'resources')

# Identify dataframes used in dashboard
snodas_swe = pd.DataFrame()
snodas_sd = pd.DataFrame()
moddrfs_forc = pd.DataFrame()
SBSP_iv = SBSP_dv = pd.DataFrame()
SASP_iv = SASP_dv = pd.DataFrame()
PTSP_iv = PTSP_dv = pd.DataFrame()
SBSG_iv = SBSG_dv = pd.DataFrame()

### Import Database Data ###
# Parse files (select csv files, open, append date, append to database)
for data_file in data_files:
    if "zip" in data_file:
        continue
    if ".db" in data_file:
        continue
    if ".py" in data_file:
        continue
    if "db" in data_file:
        data_file_split = data_file.split("_")
        source = data_file_split[0]
        datatype = data_file_split[1]

        if source == "snodas":
            continue
        #     if datatype == "swe":
        #         snodas_swe = db_import(data_file)
        #     if datatype == "snowdepth":
        #         snodas_sd = db_import(data_file)
        # if source == "moddrfs":
        #     if datatype == "forc":
        #         moddrfs_forc = db_import(data_file)
        #     if datatype == "grnsz":
        #         # moddrfs_grnsz = db_import(data_file) # not used yet
        #         continue
        if source == "modscag":
            # Skip for now
            continue
        if source == "snowreporters":
            # Import as list of SNOTEL sites (ignore actual data)
            # reporters = pd.read_csv(data_file)
            # snotel_raw = reporters[reporters["datatype"]=="SNOTEL"]
            # snotel_raw = snotel_raw.reset_index()
            continue
        if source == "SBSP":
            if datatype == "1hr":
                SBSP_iv = csas_db_import(data_file, datatype)
            if datatype == "24hr":
                SBSP_dv = csas_db_import(data_file, datatype)
        if source == "SASP":
            if datatype == "1hr":
                SASP_iv = csas_db_import(data_file, datatype)
            if datatype == "24hr":
                SASP_dv = csas_db_import(data_file, datatype)
        if source == "PTSP":
            if datatype == "1hr":
                PTSP_iv = csas_db_import(data_file, datatype)
            if datatype == "24hr":
                PTSP_dv = csas_db_import(data_file, datatype)
        if source == "SBSG":
            if datatype == "1hr":
                SBSG_iv = csas_db_import(data_file, datatype)
            if datatype == "24hr":
                SBSG_dv = csas_db_import(data_file, datatype)
        print("Importing {} from {}".format(datatype, source))

#switch working dir back to main dir so dash app can function correctly
os.chdir(app_dir)	  

print('Calculating bounds of SNODAS.db')
# Create list of basins
# df_local_ids = pd.read_sql(
# 'select distinct LOCAL_ID as LOCAL_ID, LOCAL_NAME from sd', db.engine).dropna()
# basin_list = list()
# print(f'  {len(df_local_ids)} - distinct basins {df_local_ids["LOCAL_ID"].to_list()}')
# for i, b in df_local_ids.iterrows():
#     basin_list.append({"label": b["LOCAL_NAME"], "value": b['LOCAL_ID']})
basin_list = [
    {'label': 'SAN JUAN - NAVAJO RES NR ARCHULETA', 'value': 'NVRN5L_F'}, 
    {'label': 'ANIMAS - DURANGO', 'value': 'DRGC2H_F'}, 
    {'label': 'DOLORES - MCPHEE RESERVOIR', 'value': 'MPHC2L_F'}, 
    {'label': 'FLORIDA - LEMON RES NR DURANGO', 'value': 'LEMC2H_F'}, 
    {'label': 'LOS PINOS - NR BAYFIELD VALLECITO RES', 'value': 'VCRC2H_F'}
]
# Set ranges of variables for use in dashboard
# max_el = pd.read_sql('select max(elev_ft) from sd', db.engine).iloc[0][0]
# min_el = pd.read_sql('select min(elev_ft) from sd', db.engine).iloc[0][0]
# elevrange = [min_el, max_el]
elevrange =[6079.0, 13924.0]
print(f'  Elevations from {elevrange[0]} to {elevrange[-1]}')
elevdict = dict()
for e in range(1, 20):
    elevdict[str(e * 1000)] = f"{e * 1000:,}'"

# max_slope = pd.read_sql('select max(slope_d) from sd', db.engine).iloc[0][0]
# min_slope = pd.read_sql('select min(slope_d) from sd', db.engine).iloc[0][0]
# sloperange = [min_slope, max_slope]
sloperange = [0.0, 79.0]
print(f'  Slopes from {sloperange[0]} to {sloperange[-1]}')
slopedict = dict()
for s in range(0, 11):
    slopedict[str(s * 10)] = f'{s * 10}°'
    
aspectdict = {-90: "W",
              -45: "NW",
              0: "N",
              45: "NE",
              90: "E",
              135: "SE",
              180: "S",
              225: "SW",
              270: "W",
              315: "NW",
              360: "N"}

# Import USGS gages and define list for dashboard drop down & add colors
usgs_gages = pd.read_csv(os.path.join(res_dir, "usgs_gages.csv"))
usgs_gages.index = usgs_gages.site_no
usgs_gages["color"] = sns.color_palette("colorblind", len(usgs_gages)).as_hex()

# Add list for dropdown menu
usgs_list = list()
for g in usgs_gages.index:
    usgs_list.append({"label": "0" + str(usgs_gages.site_no[g]) + " " + usgs_gages.name[g] + " (" + str(
        usgs_gages.elev_ft[g]) + " ft | " + str(usgs_gages.area[g]) + " sq.mi.)", "value": "0" + str(g)})

# Create list of SNOTEL sites & add colors
snotel_gages = pd.read_csv(os.path.join(res_dir,"snotel_gages.csv"))
snotel_gages.index = snotel_gages.triplet
snotel_gages["color"] = sns.color_palette("colorblind", len(snotel_gages)).as_hex()
snotel_gages["prcp_color"] = sns.color_palette("pastel", len(snotel_gages)).as_hex()

# Add list for dropdown menu
snotel_list = list()
for s in snotel_gages.index:
    snotel_list.append({"label": str(snotel_gages.site_no[s]) + " " + snotel_gages.name[s] + " (" + str(
        round(snotel_gages.elev_ft[s], 0)) + " ft)", "value": s})

# Create list of CSAS sites & add colors
csas_gages = pd.DataFrame()
csas_gages["site"] = ["SASP","SBSP","PTSP","SBSG"]
csas_gages["name"] = ["Swamp Angel","Senator Beck","Putney [Meteo]","Senator Beck Gage [Flow]"]
csas_gages["elev_ft"] = [11060,12186,12323,11030]
csas_gages["color"] = sns.color_palette("dark", len(csas_gages)).as_hex()
csas_gages.index = csas_gages["site"]

csas_list = list()
csas_db = list()

for c in csas_gages.index:
    csas_list.append({"label": csas_gages.name[c] + " (" + str(
        round(csas_gages.elev_ft[c], 0)) + " ft)", "value": c})
    if c == "SBSP":
        csas_db.append({"value": c, "daily": SBSP_dv, "inst": SBSP_iv})
    if c == "SASP":
        csas_db.append({"value": c, "daily": SASP_dv, "inst": SASP_iv})
    if c == "PTSP":
        csas_db.append({"value": c, "daily": PTSP_dv, "inst": PTSP_iv})
    if c == "SBSG":
        csas_db.append({"value": c, "daily": SBSG_dv, "inst": SBSG_iv})

# Import CSAS dust on snow data
try:
    dust = pd.read_csv(os.path.join(res_dir, "csas_dust.csv"))
except FileNotFoundError:
    dust = pd.DataFrame()
if dust.empty:
    dust_disable = True
else:
    dust_disable = False
    dust_ts = dust.loc[1:len(dust),]
    dust_ts = dust_ts.reset_index(drop=True)
    dust_ts["Date"] = pd.to_datetime(dust_ts["Date"],format="%d-%m-%y")
    dust_ts.index = dust_ts.Date
    dust_ts = dust_ts.drop("Date",axis=1)
    dust_ts = (dust_ts.apply(pd.to_numeric)/2.54)

dust_layers = pd.DataFrame(index=dust_ts.columns)
dust_layers["color"] = sns.color_palette("dark", len(dust_layers)).as_hex()

# Radiative forcing check
if moddrfs_forc.empty:
    forc_disable = True
else:
    forc_disable = False\


# set initial start and end date
start_date = dt.datetime.now().date() - dt.timedelta(days=10)
end_date = dt.datetime.now().date() + dt.timedelta(days=10)

