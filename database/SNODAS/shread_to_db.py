# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

@author: buriona
"""

import sys
from pathlib import Path
import pandas as pd
# import sqlalchemy as sql
import sqlite3
import zipfile
from zipfile import ZipFile

ZIP_IT = False
ZIP_FRMT = zipfile.ZIP_LZMA
VERBOSE = False
COL_TYPES = {
    'Date': str, 'Type': str, 'OBJECTID': int, 'elev_ft': int, 'slope_d': int, 
    'aspct': str, 'nlcd': int, 'LOCAL_ID': str, 'LOCAL_NAME': str, 'mean': float
}

this_dir = Path(__file__).absolute().resolve().parent
data_dir = Path(this_dir, 'shread_data')
swe_df_list = []
sd_df_list = []
print('Preparing .csv files for database creation...\n')
for data_file in data_dir.glob('*.csv'):
    if VERBOSE:
        print(f'Adding {data_file.name} to dataframe...')
    df = pd.read_csv(
        data_file, 
        usecols=COL_TYPES.keys(),
        parse_dates=['Date'],
        dtype=COL_TYPES
    )
    if not df.empty:
        swe_df_list.append(
            df[df['Type'] == 'swe'].drop(columns='Type').copy()
        )
        sd_df_list.append(
            df[df['Type'] == 'snowdepth'].drop(columns='Type').copy()
        )
        
df_swe = pd.concat(swe_df_list)
df_swe.name = 'swe'
df_sd = pd.concat(sd_df_list)
df_sd.name = 'sd'

for df in [df_swe, df_sd]:
    sensor = df.name
    print(f'Creating sqlite db for {df.name}...\n')
    print('  Getting unique basin names...')
    basin_list = pd.unique(df['LOCAL_NAME'])
    db_name = f"{sensor}.db"
    db_path = Path(this_dir, db_name)
    zip_name = f"{sensor}_db.zip"
    zip_path = Path(this_dir, zip_name)
    print(f"  Creating {db_name}")
    df_basin = None
    con = None
    try:
        con = sqlite3.connect(db_path)
        for basin in basin_list:
            print(f'    Getting data for {basin}...')
            df_basin = df[df['LOCAL_NAME'] == basin]
            if df_basin.empty:
                print(f'      No data for {basin}...')
                continue
            basin_id = df['LOCAL_ID'].iloc[0]
            print(f'      Writting {basin} to {db_name}...')
            try:
                df_basin.to_sql(basin_id, con, if_exists='replace')
            except Exception as err:
                print(f'      Failed writting {basin} data to {db_name} - {err}')
            
    except sqlite3.Error as e:
        print(f'      Error - did not complete writting to {db_name} - {e}')
    finally:
        if con:
            con.close()
    if ZIP_IT:
        print('  When a problem comes along you must zip it! - ({zip_name})')
        with ZipFile(zip_path.as_posix(), 'w', compression=ZIP_FRMT) as z:
            z.write(db_path.as_posix())