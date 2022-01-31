# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

Compiles CSAS data into SQLite DB

@author: buriona,tclarkin
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import sqlite3
import sqlalchemy as sql
import zipfile
from zipfile import ZipFile
from requests import get as r_get
from io import StringIO

# Load directories and defaults
this_dir = Path(__file__).absolute().resolve().parent
#this_dir = Path('C:/Programs/shread_dash/database/CSAS')
ZIP_IT = False
ZIP_FRMT = zipfile.ZIP_LZMA
DEFAULT_DATE_FIELD = 'date'
DEFAULT_ARCH_DIR = Path(this_dir,'csas_archive')
DEFAULT_CSV_DIR = Path(this_dir,'data')
DEFAULT_DB_DIR = this_dir
COL_TYPES = {
    'date': str,'site':str,'type':str,'albedo':float,'snwd':float,'temp':float,'flow':float
}

# Define functions
def compose_date(years, months=1, days=1, weeks=None, hours=None, minutes=None,
                 seconds=None, milliseconds=None, microseconds=None, nanoseconds=None):
    """
    for composing dates from arrays of specified quantities (years, months, etc)
    output: datetime array
    """
    years = np.asarray(years) - 1970
    months = np.asarray(months) - 1
    days = np.asarray(days) - 1
    types = ('<M8[Y]', '<m8[M]', '<m8[D]', '<m8[W]', '<m8[h]',
             '<m8[m]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]')
    vals = (years, months, days, weeks, hours, minutes, seconds,
            milliseconds, microseconds, nanoseconds)
    return sum(np.asarray(v, dtype=t) for t, v in zip(types, vals)
               if v is not None)

def process_csas_archive(data_dir=DEFAULT_CSV_DIR,csas_archive=DEFAULT_ARCH_DIR,verbose=False):
    """
    process csas archive data (.csv in DEFAULT_ARCH_DIR) from https://snowstudies.org/archived-data/
    output: none in consul, formatted .csv files to DEFAULT_CSV_DIR
    """
    # Check for output directory:
    if os.path.isdir(data_dir) is False:
        os.mkdir(data_dir)

    print('Preparing processed csas .csv files for database creation...')
    # Check for input data
    if os.path.isdir(csas_archive) is False:
        print("No archive data provided...")
        return

    if csas_archive.glob("*.csv") is []:
        print("No archive data provided...")
        return

    for archive_file in csas_archive.glob("*.csv"):
        if "dust" in str(archive_file):
            continue
        else:
            file = str(archive_file).replace(str(csas_archive),"").replace("\\","")
            site = file.split("_")[0]
        if verbose:
            print(f'Processing {archive_file.name}...')
        df_in = pd.read_csv(archive_file)

        # Create output df
        df_out = pd.DataFrame(index=df_in.index)
        df_out["site"] = site
        # Check dates
        if "24hr" in str(archive_file):
            dtype = "dv"
            dates = compose_date(years=df_in.Year, days=df_in.DOY)
        if "1hr" in str(archive_file):
            dtype = "iv"
            dates = compose_date(years=df_in.Year, days=df_in.DOY, hours=df_in.Hour / 100)

        df_out["type"] = dtype

        # Check for albedo
        if ("PyDwn_Unfilt_W" in str(df_in.columns)) and ("PyUp_Unfilt_W" in str(df_in.columns)):
            df_out["albedo"] = df_in["PyDwn_Unfilt_W"] / df_in["PyUp_Unfilt_W"]
            df_out.loc[df_out["albedo"] > 1, "albedo"] = 1
            df_out.loc[df_out["albedo"] < 0, "albedo"] = 0
        else:
            df_out["albedo"] = np.nan

        # Check for snow depth
        if ("Sno_Height_M" in str(df_in.columns)):
            df_out["snwd"] = df_in["Sno_Height_M"]*3.281*12 # Convert to inches
        else:
            df_out["snwd"] = np.nan

        # Check for temperature
        if dtype=="dv":
            if ("UpAir_Avg_C" in str(df_in.columns)):
                df_out["temp"] = df_in["UpAir_Avg_C"]*9/5+32  # Convert to F
            elif ("Air_Max_C" in str(df_in.columns)) & ("Air_Min_C" in str(df_in.columns)):
                df_out["temp"] =(df_in["Air_Max_C"]/2+df_in["Air_Min_C"]/2)*9/5+32  # Convert to F
            else:
                df_out["temp"] = np.nan
        elif dtype=="iv":
            if ("UpAir_Max_C" in str(df_in.columns)):
                df_out["temp"] = df_in["UpAir_Max_C"]*9/5+32  # Convert to F
            elif ("Air_Max_C" in str(df_in.columns)):
                df_out["temp"] = df_in["Air_Max_C"]*9/5+32  # Convert to F
            else:
                df_out["temp"] = np.nan

        # Check for flow
        if ("Discharge" in str(df_in.columns)):
            df_out["flow"] = df_in["Discharge_CFS"]
        else:
            df_out["flow"] = np.nan

        df_out.index = dates

        df_out.to_csv(Path(data_dir,file),index_label="date")

def process_csas_live(data_dir=DEFAULT_CSV_DIR,verbose=False):
    """
    process csas live data from https://snowstudies.org/current-conditions/
    output: none in consul, formatted .csv files to DEFAULT_CSV_DIR
    """
    # Check for output directory:
    if os.path.isdir(data_dir) is False:
        os.mkdir(data_dir)

    csas_sites = ["SBSP","SASP","PTSP","SBSG"]
    csas_dtypes = ["iv","dv"]
    # Set filepath extension for dtype

    for dtype in csas_dtypes:
        if verbose:
            print(f'Processing {dtype} data...')

        if dtype == "iv":
            ext = "Hourly.php"
        if dtype == "dv":
            ext = "Daily.php"

        for site in csas_sites:
            if verbose:
                print(f'for {site}...')

            # Construct url
            site_url = "https://www.snowstudies.info/NRTData/" + site + "Full" + ext
            print(site_url)

            # Import
            failed = True
            tries = 0
            while failed:
                try:
                    csv_str = r_get(site_url,timeout=None,verify=False).text
                    failed = False
                except TimeoutError:
                    raise Exception("Timeout; Data unavailable?")
                    tries += 1
                    print(tries)
                    if tries > 15:
                        return

            csv_io = StringIO(csv_str)
            try:
                f = pd.read_html(csv_io)
            except ValueError:
                return

            df_in = f[1]
            if df_in.empty:
                if verbose:
                    print("Data not available")
                    return
            if df_in is None:
                if verbose:
                    print("Data not available")
                    return

            if dtype == "dv":
                dates = compose_date(years=df_in.Year,days=df_in.Day)
            if dtype == "iv":
                dates = compose_date(years=df_in.Year, days=df_in.Day,hours=df_in.Hour/100)

            df_out = pd.DataFrame(index=df_in.index,
                                  columns=["site","type","albedo","snwd","temp","flow"])
            df_out["site"] = site
            df_out["type"] = dtype

            for col in df_in.columns:
                # Check for albedo and solar radiation
                if "Albedo" in col:
                    df_out["albedo"] = df_in[col]
                if "Solar Radiation-Up" in col:
                    df_out["radup"] = df_in[col]
                if "Solar Radiation-Down" in col:
                    df_out["raddn"] = df_in[col]

                # Check for snow depth
                if "Snow Depth" in col:
                    df_out["snwd"] = df_in[col]*3.281*12
                    df_out.loc[df_out["snwd"] > 109, 'snwd'] = np.nan  # 109 is common error value
                    df_out["snwd"] = df_out["snwd"].interpolate(limit=3)

                # Check for temp
                if ("Air Temperature" in col) & ("(C" in col):
                    df_out["temp"] = df_in[col]*9/5+32

                # Check for flow
                if "Discharge" in col:
                    df_out["flow"] = df_in[col]

            # Fix albedo
            if (all(pd.isna(df_out["albedo"]))==True) and ("radup" in df_out.columns) and ("raddn" in df_out.columns):
                df_out["albedo"] = df_out["raddn"] / df_out["radup"]
                df_out.loc[df_out["albedo"]>1,'albedo'] = 1
                df_out.loc[df_out["albedo"]<0,"albedo"] = 0
            if ("radup" in df_out.columns):
                df_out = df_out.drop(labels=["radup"],axis=1)
            if ("raddn" in df_out.columns):
                df_out = df_out.drop(labels=["raddn"], axis=1)

            # Add date index
            df_out.index=dates

            file = f"{site}_{dtype}_live.csv"
            df_out.to_csv(Path(data_dir,file),index_label="date")

def get_dfs(data_dir=DEFAULT_CSV_DIR,verbose=False):
    """
    Get and merge dataframes imported using functions
    """
    csas_iv_df_list = []
    csas_dv_df_list = []
    print('Preparing .csv files for database creation...')
    for data_file in data_dir.glob('*.csv'):
        if verbose:
            print(f'Adding {data_file.name} to dataframe...')
        df = pd.read_csv(
            data_file, 
            usecols=COL_TYPES.keys(),
            parse_dates=['date'],
            dtype=COL_TYPES
        )
        if not df.empty:
            csas_iv_df_list.append(
                df[df['type'] == 'iv'].drop(columns='type').copy()
            )
            csas_dv_df_list.append(
                df[df['type'] == 'dv'].drop(columns='type').copy()
            )
            
    df_csas_iv = pd.concat(csas_iv_df_list)
    df_csas_iv.name = 'csas_iv'
    df_csas_dv = pd.concat(csas_dv_df_list)
    df_csas_dv.name = 'csas_dv'
    print('  Success!!!\n')
    return {'csas_iv':df_csas_iv,'csas_dv':df_csas_dv}

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
    print('  Getting unique site names...')
    site_list = pd.unique(df['site'])
    db_name = f"{sensor}.db"
    db_path = Path(db_path, db_name)
    zip_name = f"{sensor}_db.zip"
    zip_path = Path(db_path, zip_name)
    print(f"  Writing {db_path}...")
    df_site = None
    con = None
    for site in site_list:
        if verbose:
            print(f'    Getting data for {site}...')
        df_site = df[df['site'] == site]
        if df_site.empty:
            if verbose:
                print(f'      No data for {site}...')
            continue
        
        site_id = site
        if if_exists == 'append' and check_dups:
            if verbose:
                print(f'      Checking for duplicate data in {site}...')
            unique_dates = get_unique_dates(site_id, db_path)
            initial_len = len(df_site.index)
            df_site = df_site[~df_site[DEFAULT_DATE_FIELD].isin(unique_dates)]
            if verbose:
                print(f'        Prevented {initial_len - len(df_site.index)} duplicates')
        if verbose:
            print(f'      Writing {site} to {db_name}...')
        try:
            con = sqlite3.connect(db_path)
            df_site.to_sql(
                site_id,
                con, 
                if_exists=if_exists,
                chunksize=10000,
                method='multi'
            )
        except sqlite3.Error as e:
            print(f'      Error - did not write {site_id} table to {db_name} - {e}')
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
    cli_desc = '''Creates sqlite db files for SHREAD swe and sd datatypes 
    from SHREAD output'''
    
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument(
        "-V",
        "--version",
        help="show program version",
        action="store_true"
    )
    parser.add_argument(
        "-i", "--input", 
        help=f"override default csas data input dir ({DEFAULT_CSV_DIR})",
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
        choices=['replace','append','fail'],
        default='append'
    )
    parser.add_argument(
        "-c", "--check_dups", 
        help="only write non-duplicate dates (can slow process ALOT!)",
        action='store_true',
        default='true'
    )
    parser.add_argument(
        "-z", "--zip", 
        help='zip database files after creation',
        action="store_true"
    )
    parser.add_argument(
        "--verbose",
        help="print/log verbose",
        action="store_true",
        default="true")
    return parser.parse_args()

if __name__ == '__main__':
    """
    Actual batch file run script
    """

    import argparse

    # Process archived csas data
    process_csas_archive()

    # Process live csas data
    process_csas_live()

    # Arguments for db build
    args = parse_args()
    print(args)

    if args.version:
        print('csas_to_db.py v1.0')
    
    for arg_path in [args.input, args.output]:
        if not Path(arg_path).is_dir():
            print('Invalid arg filepath ({args_path}), please try again.')
            sys.exit(1)
        
    df_dict = get_dfs(Path(args.input), verbose=args.verbose)
    df_csas_iv = df_dict['csas_iv']
    df_csas_dv = df_dict['csas_dv']
    
    for df in [df_csas_iv, df_csas_dv]:
        write_db(
            df, 
            if_exists=args.exists, 
            check_dups=args.check_dups,
            zip_db=args.zip, 
            verbose=args.verbose
        )
    