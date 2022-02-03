# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 11:55:27 2021

Snow-Hydrology Repo for Evaluation, Analysis, and Decision-making Dashboard (shread_dash.py) Database Initialization

This is part of dashboard loading database and other data into memory. The data for the database relies on a series of
retrieval scripts (/database/SUBS) that retrieve hydrometeorological data from online and store the data in local
databases. Part of the retrieval process is dependent on the SHREAD repository (https://github.com/tclarkin/shread).
The databases are built in SQLite.

@author: tclarkin, buriona (2020-2022)

"""

import os
import datetime as dt
from pathlib import Path
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
import dash_bootstrap_components as dbc
import dash

### Launch SQLite DB Server ###
# Define directories and app
this_dir = os.path.dirname(os.path.realpath(__file__))
#this_dir = Path('C:/Programs/shread_dash/database')
app_dir = os.path.dirname(this_dir)

# define functions
def create_app():
    """
    This function launches the SALAlchemy db server
    """
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
    snodas_swe_db_path = Path(db_path, 'SHREAD', 'swe.db')
    snodas_sd_db_path = Path(db_path, 'SHREAD', 'sd.db')
    csas_iv_db_path = Path(db_path, 'CSAS', 'csas_iv.db')
    csas_dv_db_path = Path(db_path, 'CSAS', 'csas_dv.db')
    snotel_dv_db_path = Path(db_path, 'SNOTEL', 'snotel_dv.db')
    usgs_dv_db_path = Path(db_path, 'FLOW', 'usgs_dv.db')
    usgs_iv_db_path = Path(db_path, 'FLOW', 'usgs_iv.db')
    rfc_dv_db_path = Path(db_path, 'FLOW', 'rfc_dv.db')
    rfc_iv_db_path = Path(db_path, 'FLOW', 'rfc_iv.db')
    ndfd_mint_db_path = Path(db_path, 'SHREAD', 'mint.db')
    ndfd_maxt_db_path = Path(db_path, 'SHREAD', 'maxt.db')
    ndfd_rhm_db_path = Path(db_path, 'SHREAD', 'rhm.db')
    ndfd_pop12_db_path = Path(db_path, 'SHREAD', 'pop12.db')
    ndfd_qpf_db_path = Path(db_path, 'SHREAD', 'qpf.db')
    ndfd_snow_db_path = Path(db_path, 'SHREAD', 'snow.db')
    ndfd_sky_db_path = Path(db_path, 'SHREAD', 'sky.db')

    snodas_swe_db_con_str = f'sqlite:///{snodas_swe_db_path.as_posix()}'
    snodas_sd_db_con_str = f'sqlite:///{snodas_sd_db_path.as_posix()}'
    csas_iv_db_con_str = f'sqlite:///{csas_iv_db_path.as_posix()}'
    csas_dv_db_con_str = f'sqlite:///{csas_dv_db_path.as_posix()}'
    snotel_dv_db_con_str = f'sqlite:///{snotel_dv_db_path.as_posix()}'
    usgs_dv_db_con_str = f'sqlite:///{usgs_dv_db_path.as_posix()}'
    usgs_iv_db_con_str = f'sqlite:///{usgs_iv_db_path.as_posix()}'
    rfc_dv_db_con_str = f'sqlite:///{rfc_dv_db_path.as_posix()}'
    rfc_iv_db_con_str = f'sqlite:///{rfc_iv_db_path.as_posix()}'
    ndfd_mint_db_con_str = f'sqlite:///{ndfd_mint_db_path}'
    ndfd_maxt_db_con_str = f'sqlite:///{ndfd_maxt_db_path}'
    ndfd_rhm_db_con_str = f'sqlite:///{ndfd_rhm_db_path}'
    ndfd_pop12_db_con_str = f'sqlite:///{ndfd_pop12_db_path}'
    ndfd_qpf_db_con_str = f'sqlite:///{ndfd_qpf_db_path}'
    ndfd_snow_db_con_str = f'sqlite:///{ndfd_snow_db_path}'
    ndfd_sky_db_con_str = f'sqlite:///{ndfd_sky_db_path}'

    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.server.config['SQLALCHEMY_BINDS'] = {
        'swe': snodas_swe_db_con_str,
        'sd': snodas_sd_db_con_str,
        'csas_iv':csas_iv_db_con_str,
        'csas_dv':csas_dv_db_con_str,
        'snotel_dv':snotel_dv_db_con_str,
        'usgs_dv':usgs_dv_db_con_str,
        'usgs_iv':usgs_iv_db_con_str,
        'rfc_dv':rfc_dv_db_con_str,
        'rfc_iv':rfc_iv_db_con_str,
        "mint": ndfd_mint_db_con_str,
        "maxt": ndfd_maxt_db_con_str,
        "rhm": ndfd_rhm_db_con_str,
        "pop12": ndfd_pop12_db_con_str,
        "qpf": ndfd_qpf_db_con_str,
        "snow": ndfd_snow_db_con_str,
        "sky": ndfd_sky_db_con_str,
    }

    return app

# Launch server
app = create_app()
db = SQLAlchemy(app.server)
db.reflect()

### Load in other Data ###
# Define working (data) directory
os.chdir(os.path.join(app_dir, 'database'))

# Identify files in database
csas_dir = os.path.join(app_dir, 'database', 'CSAS')
csas_files = os.listdir(csas_dir)
res_dir = os.path.join(app_dir, 'resources')

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
    {'label': 'NONE', 'value': None},
    {'label': 'SAN JUAN - NAVAJO RES NR ARCHULETA', 'value': 'NVRN5L_F'}, 
    {'label': 'ANIMAS - DURANGO', 'value': 'DRGC2H_F'}, 
    {'label': 'DOLORES - MCPHEE RESERVOIR', 'value': 'MPHC2L_F'}, 
    {'label': 'FLORIDA - LEMON RES NR DURANGO', 'value': 'LEMC2H_F'}, 
    {'label': 'LOS PINOS - NR BAYFIELD VALLECITO RES', 'value': 'VCRC2H_F'}
]
# Set ranges of variables for use in dashboard
elevrange =[5000, 15000]
print(f'  Elevations from {elevrange[0]} to {elevrange[-1]}')
elevdict = dict()
for e in range(1, 20):
    elevdict[str(e * 1000)] = f"{e * 1000:,}'"

sloperange = [0.0, 100]
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

# Define colors:
# https://colorbrewer2.org/?type=qualitative&scheme=Set1&n=9
color8 = ['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#a65628','#f781bf','#999999']
# Import FLOW gages and define list for dashboard drop down & add colors
usgs_gages = pd.read_csv(os.path.join(this_dir,"FLOW", "usgs_gages.csv"))
usgs_gages.index = usgs_gages.site_no
colorg = color8
while len(colorg)<len(usgs_gages):
    colorg = colorg*2
usgs_gages["color"] = colorg[0:len(usgs_gages)]

# Add list for dropdown menu
usgs_list = list()
for g in usgs_gages.index:
    usgs_list.append({"label": "0" + str(usgs_gages.site_no[g]) + " " + usgs_gages.name[g] + " (" + str(
        usgs_gages.elev_ft[g]) + " ft | " + str(usgs_gages.area[g]) + " sq.mi.)", "value": "0" + str(g)})

# Create list of SNOTEL sites & add colors
snotel_sites = pd.read_csv(os.path.join(this_dir,"SNOTEL","snotel_sites.csv"))
snotel_sites.index = snotel_sites.triplet
colors = color8
while len(colors)<len(snotel_sites):
    colors = colors*2
snotel_sites["color"] = snotel_sites["prcp_color"] = colors[0:len(snotel_sites)]

# Add list for dropdown menu
snotel_list = list()
for s in snotel_sites.index:
    snotel_list.append({"label": str(snotel_sites.site_no[s]) + " " + snotel_sites.name[s] + " (" + str(
        round(snotel_sites.elev_ft[s], 0)) + " ft)", "value": s})

# Create list of CSAS sites & add colors
csas_gages = pd.DataFrame()
csas_gages["site"] = ["SASP","SBSP","PTSP","SBSG"]
csas_gages["name"] = ["Swamp Angel","Senator Beck","Putney [Meteo]","Senator Beck Gage [Flow]"]
csas_gages["elev_ft"] = [11060,12186,12323,11030]
colorc = color8
while len(colorc)<len(csas_gages):
    colorc = colorc*2
csas_gages["color"] = csas_gages["prcp_color"] = colorc[0:len(csas_gages)]
csas_gages.index = csas_gages["site"]

csas_list = list()
for c in csas_gages.index:
    csas_list.append({"label": csas_gages.name[c] + " (" + str(
        round(csas_gages.elev_ft[c], 0)) + " ft)", "value": c})

# Generate NDFD list
forecast_list = [{"label":"Flow (RFC)","value":"flow"},
             {"label":"Min. Temp","value":"mint"},
             {"label":"Max. Temp","value":"maxt"},
             {"label":"Precip (QPF)","value":"qpf"},
             {"label": "Precip Prob.", "value": "pop12"},
             {"label":"Snow","value":"snow"},
             {"label":"Relative Humidity","value":"rhm"},
             {"label":"Sky Coverage","value":"sky"}
             ]

# Import CSAS dust on snow data
try:
    dust = pd.read_csv(os.path.join(csas_dir, "csas_dust.csv"))
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

    colord = color8
    while len(colord) < len(dust_layers):
        colord = colord * 2
    dust_layers["color"] = colord[0:len(dust_layers)]

# set initial start and end date
start_date = dt.datetime.now().date() - dt.timedelta(days=10)
end_date = dt.datetime.now().date() + dt.timedelta(days=10)

