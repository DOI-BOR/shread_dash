# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

Compiles RFC data into SQLite DB

@author: buriona,tclarkin
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import sqlalchemy as sql
import zipfile
from zipfile import ZipFile
from requests import get as r_get
from io import StringIO
import dataretrieval.nwis as nwis

# Load directories and defaults
this_dir = Path(__file__).absolute().resolve().parent
#this_dir = Path('C:/Programs/shread_dash/database/FLOW')
ZIP_IT = False
ZIP_FRMT = zipfile.ZIP_LZMA
DEFAULT_DATE_FIELD = 'date'
DEFAULT_CSV_DIR = Path(this_dir,'rfc_data')
DEFAULT_DB_DIR = this_dir

# TODO check this!
COL_TYPES = {
    'date': str,'flow':float,'site':str,'type':str,'fcst_dt':str
}

def import_rfc(site,dtype,rfc = "cbrfc",data_dir=None,verbose=False):
    """Download NWS RFC flow data

    Parameters
    ---------
        site: five digit site identifier
        start_date: datetime
        end_date: datetime
        time_int: text
        verbose: boolean
            True : enable print during function run

    Returns
    -------
        dataframe

    """
    if dtype == "dv":
        ext = ".fflw24.csv"
    if dtype == "iv":
        ext = ".fflw1.csv"

    site_url = "https://www."+rfc+".noaa.gov/product/hydrofcst/RVFCSV/"+site+ext

    try:
        csv_str = r_get(site_url, timeout=10).text
    except TimeoutError:
        raise Exception("Timeout; Data unavailable?")
    if "not found on this server" in csv_str:
        print("Site URL incorrect.")
        return None

    csv_io = StringIO(csv_str)
    rfc_in = csv_io.readlines()
    rfc_dat = pd.DataFrame()
    data=False
    i = 0
    for line in range(0, len(rfc_in)):
        text = str(rfc_in[line])
        text = text.rstrip()
        if text[:4] == "DATE":
            columns = text.split(",")
            data=True
            if verbose == True:
                print(columns)
            continue
        if data==True:
            vals = text.split(",")
            if verbose == True:
                print(vals)
            for col in range(0, len(columns)):
                rfc_dat.loc[i, columns[col]] = vals[col]
            i = i + 1

    rfc_dat["DATE"] = pd.to_datetime(rfc_dat["DATE"])
    rfc_dat["flow"] = pd.to_numeric(rfc_dat["FLOW"])

    for i in rfc_dat.index:
        rfc_dat.loc[i,"datetime"] = rfc_dat.loc[i,"DATE"]+dt.timedelta(hours=int(rfc_dat.loc[i,"TIME"].strip("Z")))

    rfc_dat.index = rfc_dat.datetime
    rfc_dat = rfc_dat.drop(columns=["DATE","TIME","datetime","FLOW"])
    rfc_dat = rfc_dat.tz_localize("UTC")
    fcst_dt = rfc_dat.index.min().date().strftime("%Y-%m-%d")

    if data_dir is None:
        return (rfc_dat,fcst_dt)
    else:
        if os.path.isdir(data_dir) is False:
            os.mkdir(data_dir)


        rfc_dat["site"] = site
        rfc_dat["type"] = f"rfc_{dtype}"
        rfc_dat["fcst_dt"] = fcst_dt
        rfc_dat.to_csv(Path(data_dir,f"{site}_{dtype}_{fcst_dt}.csv"), index_label="date")

        return fcst_dt

def get_dfs(data_dir=DEFAULT_CSV_DIR,verbose=False):
    """
    Get and merge dataframes imported using functions
    """
    rfc_df_dv_list = []
    rfc_df_iv_list = []
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
            rfc_df_iv_list.append(
                df[df['type'] == 'rfc_iv'].drop(columns='type').copy()
            )
            rfc_df_dv_list.append(
                df[df['type'] == 'rfc_dv'].drop(columns='type').copy()
            )

    df_rfc_dv = pd.concat(rfc_df_dv_list)
    df_rfc_dv.name = 'rfc_dv'
    df_rfc_iv = pd.concat(rfc_df_iv_list)
    df_rfc_iv.name = 'rfc_iv'
    print('  Success!!!\n')
    return {'rfc_dv':df_rfc_dv,
            'rfc_iv':df_rfc_iv}

def get_unique_dates(tbl_name, db_path, date_field=DEFAULT_DATE_FIELD):
    """
    Get unique dates from rfc data, to ensure no duplicates
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
             zip_db=ZIP_IT, zip_frmt=ZIP_FRMT, verbose=True):
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
            print(f'      Writing site_{site_id} to {db_name}...')
        try:
            con = sqlite3.connect(db_path)
            df_site.to_sql(
                f"site_{site}",
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
        choices=['replace', 'append', 'fail'],
        default='replace'
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

    # Identify SNOTEL sites:
    usgs_sites = pd.read_csv(os.path.join(this_dir, "usgs_gages.csv"),index_col=0)

    for site_no in usgs_sites.index:
        if str(usgs_sites.loc[site_no,"rfc"])!="nan":
            for dtype in ["dv", "iv"]:
                print(f'Downloading data for {usgs_sites.loc[site_no,"rfc"]}')
                fcst_dt = import_rfc(usgs_sites.loc[site_no,"rfc"],dtype,"cbrfc",DEFAULT_CSV_DIR)

    # TODO: fix duplicate issues

    # Arguments for db build
    args = parse_args()
    print(args)

    if args.version:
        print('rfc_to_db.py v1.0')

    for arg_path in [args.input, args.output]:
        if not Path(arg_path).is_dir():
            print('Invalid arg filepath ({args_path}), please try again.')
            sys.exit(1)

    df_dict = get_dfs(Path(args.input), verbose=args.verbose)
    df_rfc_dv = df_dict['rfc_dv']
    df_rfc_iv = df_dict['rfc_iv']

    for df in [df_rfc_dv,df_rfc_iv]:
        write_db(
            df,
            if_exists=args.exists,
            check_dups=args.check_dups,
            zip_db=args.zip,
            verbose=args.verbose
        )
