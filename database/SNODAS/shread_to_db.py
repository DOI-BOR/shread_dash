# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

@author: buriona,tclarkin
"""

import sys
from pathlib import Path
import pandas as pd
import sqlalchemy as sql
import sqlite3
import zipfile
from zipfile import ZipFile

# Load directories and defaults
this_dir = Path(__file__).absolute().resolve().parent
#this_dir = Path('C:/Programs/shread_plot/database/SNODAS')
ZIP_IT = False
ZIP_FRMT = zipfile.ZIP_LZMA
DEFAULT_DATE_FIELD = 'Date'
DEFAULT_CSV_DIR = Path(this_dir, 'data')
DEFAULT_DB_DIR = this_dir
COL_TYPES = {
    'Date': str, 'Type': str, 'OBJECTID': int, 'elev_ft': int, 'slope_d': int, 
    'aspct': int, 'nlcd': int, 'LOCAL_ID': str, 'LOCAL_NAME': str, 'mean': float
}

# Define functions
def get_dfs(data_dir=DEFAULT_CSV_DIR, verbose=False):
    """
    Get and merge dataframes imported using shread.py
    """
    swe_df_list = []
    sd_df_list = []
    print('Preparing .csv files for database creation...')
    for data_file in data_dir.glob('*.csv'):
        if verbose:
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
    print('  Success!!!\n')
    return {'swe': df_swe, 'sd': df_sd}

def get_unique_dates(tbl_name, db_path, date_field=DEFAULT_DATE_FIELD):
    """
    Get unique dates from shread data, to ensure no duplicates
    """
    if not db_path.is_file():
        return pd.DataFrame(columns=[DEFAULT_DATE_FIELD])
    db_con_str = f'sqlite:///{db_path.as_posix()}'
    eng = sql.create_engine(db_con_str)
    with eng.connect() as con:
        try:
            unique_dates = pd.read_sql(
                f'select distinct {date_field} from {tbl_name}',
                con
            ).dropna()
        except Exception:
            return pd.DataFrame(columns=[DEFAULT_DATE_FIELD])
    return pd.to_datetime(unique_dates[date_field])

def write_db(df, db_path=DEFAULT_DB_DIR, if_exists='replace', check_dups=False,
              zip_db=ZIP_IT, zip_frmt=ZIP_FRMT, verbose=False):
    """
    Write dataframe to database
    """
    sensor = df.name
    print(f'Creating sqlite db for {df.name}...\n')
    print('  Getting unique basin names...')
    basin_list = pd.unique(df['LOCAL_NAME'])
    db_name = f"{sensor}.db"
    db_path = Path(db_path, db_name)
    zip_name = f"{sensor}_db.zip"
    zip_path = Path(db_path, zip_name)
    print(f"  Writing {db_path}...")
    df_basin = None
    con = None
    for basin in basin_list:
        if verbose:
            print(f'    Getting data for {basin}...')
        df_basin = df[df['LOCAL_NAME'] == basin]
        if df_basin.empty:
            if verbose:
                print(f'      No data for {basin}...')
            continue
        
        basin_id = df_basin['LOCAL_ID'].iloc[0]
        if if_exists == 'append' and check_dups:
            if verbose:
                print(f'      Checking for duplicate data in {basin}...')
            drop_dates = get_unique_dates(basin_id, db_path)
            initial_len = len(df_basin.index)
            df_basin = df_basin[~df_basin[DEFAULT_DATE_FIELD].isin(drop_dates)]
            if verbose:
                print(f'        Prevented {initial_len - len(df_basin.index)} duplicates')
        if verbose:
            print(f'      Writing {basin} to {db_name}...')
        try:
            con = sqlite3.connect(db_path)
            df_basin.to_sql(
                basin_id, 
                con, 
                if_exists=if_exists,
                chunksize=10000,
                method='multi'
            )
        except sqlite3.Error as e:
            print(f'      Error - did not write {basin_id} table to {db_name} - {e}')
        finally:
            if con:
                con.close()
                con = None
    if zip_db:
        if verbose:
            print('  When a problem comes along you must zip it! - ({zip_name})')
        with ZipFile(zip_path.as_posix(), 'w', compression=zip_frmt) as z:
            z.write(db_path.as_posix())
    print('Success!!\n')
    
def parse_args():
    """
    Arg parsing for command line use
    """
    cli_desc = '''Creates sqlite db files for SNODAS swe and sd datatypes 
    from SHREAD output'''
    
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument(
        "-V", "--version", help="show program version", action="store_true"
    )
    parser.add_argument(
        "-i", "--input", 
        help=f"override default SHREAD data input dir ({DEFAULT_CSV_DIR})",
        default=DEFAULT_CSV_DIR
    )
    parser.add_argument(
        "-o", "--output", 
        help=f"override default db output dir ({DEFAULT_DB_DIR})",
        default=DEFAULT_DB_DIR
    )
    parser.add_argument(
        "-e", "--exists", 
        help="behavior if database table exists already",
        choices=['replace', 'append', 'fail'], default='append'
    )
    parser.add_argument(
        "-c", "--check_dups", 
        help="only write non-duplicate dates (can slow process ALOT!)",
        action='store_true'
    )
    parser.add_argument(
        "-z", "--zip", 
        help='zip database files after creation',
        action="store_true"
    )
    parser.add_argument("--verbose", help="print/log verbose", action="store_true")
    return parser.parse_args()

if __name__ == '__main__':
    """
    Actual batch file run script
    """
    import argparse
    
    args = parse_args()
    print(args)

    if args.version:
        print('shread_to_db.py v1.0')
    
    for arg_path in [args.input, args.output]:
        if not Path(arg_path).is_dir():
            print('Invalid arg filepath ({args_path}), please try again.')
            sys.exit(1)
        
    df_dict = get_dfs(Path(args.input), verbose=args.verbose)
    df_swe = df_dict['swe']
    df_sd = df_dict['sd']
    
    for df in [df_swe, df_sd]:
        write_db(
            df, 
            if_exists=args.exists, 
            check_dups=args.check_dups,
            zip_db=args.zip, 
            verbose=args.verbose
        )
    