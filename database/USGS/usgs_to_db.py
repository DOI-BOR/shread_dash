# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

@author: buriona,tclarkin
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timezone
import datetime as dt
import sqlite3
import zipfile
from zipfile import ZipFile
from requests import get as r_get
from io import StringIO

# Load directories and defaults
this_dir = Path(__file__).absolute().resolve().parent
#this_dir = Path('C:/Programs/shread_plot/database/USGS')
ZIP_IT = False
ZIP_FRMT = zipfile.ZIP_LZMA
DEFAULT_DATE_FIELD = 'date'
DEFAULT_CSV_DIR = Path(this_dir,'data')
DEFAULT_DB_DIR = this_dir

# TODO check this!
COL_TYPES = {
    'date': str,'site':str,'WTEQ':float,'SNWD':float,'PREC':float,'TAVG':float
}

# Define functions
def import_usgs(site_triplet,vars=["WTEQ", "SNWD", "PREC", "TAVG"],out_dir=DEFAULT_CSV_DIR,verbose=False,):
    """Download USGS NWIS data

    Parameters
    ---------
        site_triplet: three part SNOTEL triplet (e.g., 713_CO_SNTL)
        vars: array of variables for import (tested with WTEQ, SNWD, PREC, TAVG..other options may be available)
        out_dir: str to directory to save .csv...if None, will return df
        verbose: boolean
            True : enable print during function run

    Returns
    -------
        dataframe

    """
    if dtype == "dv":
        dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')
        parameter = "00060_Mean"
    elif dtype == "iv":
        dates = pd.date_range(start_date, end_date, freq="15T", tz='UTC')
        parameter = "00060"

    # Convert usgs_end to dt for comparison to NOW()
    usgs_end = dt.datetime.strptime(end_date, "%Y-%m-%d")

    # If usgs_end > NOW(), set usgs_end for import to NOW()
    if usgs_end >= dt.datetime.now():
        usgs_end = dt.datetime.now().date()
        print("End Date is in future, flow observations will be imported until: " + str(usgs_end))
    else:
        usgs_end = dt.datetime.strftime(usgs_end, "%Y-%m-%d")
        plot_forecast = []  # no forecast data needed if dates aren't displayed

    # Create dataframes for data, names and rfc sites
    usgs_f_df = pd.DataFrame(index=dates)
    name_df = pd.DataFrame(index=usgs_sel)
    rfc_f_df = pd.DataFrame(index=usgs_sel)

    for g in usgs_sel:
        name_df.loc[g, "name"] = str(g) + " " + str(usgs_gages.loc[usgs_gages["site_no"] == int(g), "name"].item())
        if plot_forecast == True:
            rfc_f_df.loc[g, "name"] = str(usgs_gages.loc[usgs_gages["site_no"] == int(g), "rfc"].item())

        try:
            flow_in = nwis.get_record(sites=g, service=dtype, start=start_date, end=usgs_end, parameterCd="00060")
        except ValueError:
            print("Gage not found; check gage ID.")
            flow_in = pd.DataFrame(index=dates)
            flow_in[parameter] = np.nan

        if len(flow_in) == 0:
            print(f"USGS data not found for {g}.")
            flow_in = pd.DataFrame(index=dates)
            flow_in[parameter] = np.nan

        flow_in.loc[flow_in[parameter] < 0, parameter] = np.nan
        flow_in = usgs_f_df.merge(flow_in[parameter], left_index=True, right_index=True, how="left")
        usgs_f_df[g] = flow_in[parameter]







def get_dfs(data_dir=DEFAULT_CSV_DIR,verbose=False):
    """
    Get and merge dataframes imported using functions
    """
    usgs_df_list = []
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
            usgs_df_list.append(
                df
            )

    df_usgs_dv = pd.concat(usgs_df_list)
    df_usgs_dv.name = 'usgs_dv'
    df_usgs_iv = pd.concat(usgs_df_list)
    df_usgs_iv.name = 'usgs_iv'
    print('  Success!!!\n')
    return {'usgs_dv':df_usgs_dv,'usgs_iv':df_usgs_iv}

def get_unique_dates(tbl_name, db_path, date_field=DEFAULT_DATE_FIELD):
    """
    Get unique dates from usgs data, to ensure no duplicates
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
            print(f'      Writing usgs_{site_id} to {db_name}...')
        try:
            con = sqlite3.connect(db_path)
            df_site.to_sql(
                f"usgs_{site_id}",
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
    cli_desc = '''Creates sqlite db files for SNODAS swe and sd datatypes 
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
        default='false'
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
    usgs_sites = pd.read_csv(os.path.join(this_dir, "usgs_gages.csv"))

    for site_no in usgs_sites.site_no:
        import_usgs(site_no)

    # Arguments for db build
    args = parse_args()
    print(args)

    if args.version:
        print('usgs_to_db.py v1.0')

    for arg_path in [args.input, args.output]:
        if not Path(arg_path).is_dir():
            print('Invalid arg filepath ({args_path}), please try again.')
            sys.exit(1)

    df_dict = get_dfs(Path(args.input), verbose=args.verbose)
    df_usgs_dv = df_dict['usgs_dv']
    df_usgs_iv = df_dict['usgs_iv']

    for df in [df_usgs_dv,df_usgs_iv]:
        write_db(
            df,
            if_exists=args.exists,
            check_dups=args.check_dups,
            zip_db=args.zip,
            verbose=args.verbose
        )
