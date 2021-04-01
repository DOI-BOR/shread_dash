# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 14:24:20 2021

@author: buriona
"""
from pathlib import Path
import sqlalchemy as sql
import os
import pandas as pd
import sys
import sqlite3
from sqlite3 import Error
import zipfile
from zipfile import ZipFile

this_dir = os.path.dirname(os.path.realpath(__file__))
dbs_path = Path(this_dir, 'database')
snodas_db_path = Path(dbs_path, 'snodas.db')
snodas_db_con_str = f'sqlite:///{snodas_db_path.as_posix()}'
eng = sql.create_engine(snodas_db_con_str)

with eng.connect() as con:
    
    for sensor in ['swe', 'sd']:
        print(f'Workng on {sensor}...')
        print('  Getting unique basin names...')
        df_local_ids = pd.read_sql(
            f'select distinct LOCAL_ID as LOCAL_ID, LOCAL_NAME from {sensor}',
            con
        ).dropna()
            
        basin_list = df_local_ids.to_dict(orient='records')
        db_name = f"snodas_{sensor}.db"
        db_path = Path(dbs_path, db_name)
        zip_name = f"snodas_{sensor}.zip"
        zip_path = Path(dbs_path, zip_name)
        print(f"  Creating {db_name}")
        df_basin = None
        temp_con = None
        try:
            temp_con = sqlite3.connect(db_path)
            
            for basin in basin_list:
                basin_id = basin['LOCAL_ID']
                basin_name = basin['LOCAL_NAME']
                print(f'    Getting data for {basin_name}...')
                
                df_basin = pd.read_sql(
                    f"select * from {sensor} where LOCAL_ID = '{basin_id}'",
                    con,
                    parse_dates=['Date']
                )
                print(f'    Writting {basin_name} to {sensor} db...')
                df_basin.to_sql(basin_id, temp_con, if_exists='replace')
                
        except Error as e:
            print(f'      Error - could write {basin_name} table to {db_name} - {e}')
        finally:
            if temp_con:
                temp_con.close()
        print('  When a problem comes along you must zip it! - ({zip_name})')
        with ZipFile(zip_path.as_posix(), 'w', compression=zipfile.ZIP_LZMA) as myzip:
            myzip.write(db_path.as_posix())
