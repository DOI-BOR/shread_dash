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
#this_dir = Path('C:/Programs/shread_plot/database/SNOTEL')
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
def import_snotel(site_triplet,vars=["WTEQ", "SNWD", "PREC", "TAVG"],out_dir=DEFAULT_CSV_DIR,verbose=False,):
    """Download NRCS SNOTEL data

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
    # Create dictionary of variables
    snotel_dict = dict()
    ext = "DAILY"

    # Cycle through variables
    for var in vars:
        if verbose == True:
            print("Importing {} data".format(var))
        site_url = "https://www.nrcs.usda.gov/Internet/WCIS/sitedata/" + ext + "/" + var + "/" + site_triplet + ".json"
        failed = True
        tries = 0
        while failed:
            try:
                csv_str = r_get(site_url, timeout=5).text
                failed = False
            except TimeoutError:
                raise Exception("Timeout; Data unavailable?")
                tries += 1
                if tries > 4:
                    return
            if "not found on this server" in csv_str:
                print("Site URL incorrect.")
                return

        csv_io = StringIO(csv_str)
        f = pd.read_json(csv_io, orient="index")

        # Create index of dates for available data for current site
        json_index = pd.date_range(f.loc["beginDate"].item(), f.loc["endDate"].item(), freq="D", tz='UTC')
        # Create dataframe of available data (includes Feb 29)
        json_data = pd.DataFrame(f.loc["values"].item())
        # Cycle through and remove data assigned to February 29th in non-leap years
        years = json_index.year.unique()
        for year in years:
            if (year % 4 == 0) & ((year % 100 != 0) | (year % 400 == 0)):
                continue
            else:
                feb29 = dt.datetime(year=year, month=3, day=1, tzinfo=timezone.utc)
                try:
                    feb29idx = json_index.get_loc(feb29)
                    if feb29idx == 0:
                        continue
                    else:
                        json_data = json_data.drop(feb29idx)
                        if verbose == True:
                            print("February 29th data for {} removed.".format(year))
                except KeyError:
                    continue

        # Concatenate the cleaned data to the date index
        snotel_in = pd.DataFrame(index=json_index)
        snotel_in[var] = json_data.values

        # For precip, calculate incremental precip and remove negative values
        if var == "PREC":
            if verbose == True:
                print("Calculating incremental Precip.")
            snotel_in["PREC"] = snotel_in[var] - snotel_in[var].shift(1)
            snotel_in.loc[snotel_in["PREC"] < 0, "PREC"] = 0

        # Add to dict
        snotel_dict[var] = snotel_in

    if verbose == True:
        print("Checking dates")
    begin = end = pd.to_datetime(dt.datetime.now()).tz_localize("UTC")
    for key in snotel_dict.keys():
        if snotel_dict[key].index.min() < begin:
            begin = snotel_dict[key].index.min()
        if snotel_dict[key].index.max() > end:
            end = snotel_dict[key].index.max()

    dates = pd.date_range(begin,end,freq="D",tz='UTC')
    data = pd.DataFrame(index=dates)
    data["site"] = site_triplet

    if verbose == True:
        print("Preparing output")
    for key in snotel_dict.keys():
        # Merge to output dataframe
        snotel_in = data.merge(snotel_dict[key][key], left_index=True, right_index=True, how="left")
        data[key] = snotel_in[key]
        if verbose == True:
            print("Added to dataframe")

    if out_dir is None:
        return (data)
    else:
        if os.path.isdir(out_dir) is False:
            os.mkdir(out_dir)
        data.to_csv(Path(out_dir,f"{site_triplet}.csv"),index_label="date")

def get_dfs(data_dir=DEFAULT_CSV_DIR,verbose=False):
    """
    Get and merge dataframes imported using functions
    """
    snotel_df_list = []
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
            snotel_df_list.append(
                df
            )

    df_snotel_dv = pd.concat(snotel_df_list)
    df_snotel_dv.name = 'snotel_dv'
    print('  Success!!!\n')
    return {'snotel_dv':df_snotel_dv}

def get_unique_dates(tbl_name, db_path, date_field=DEFAULT_DATE_FIELD):
    """
    Get unique dates from snotel data, to ensure no duplicates
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
            print(f'      Writing snotel_{site_id} to {db_name}...')
        try:
            con = sqlite3.connect(db_path)
            df_site.to_sql(
                f"snotel_{site_id}",
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
    snotel_sites = pd.read_csv(os.path.join(this_dir, "snotel_sites.csv"))

    for site_triplet in snotel_sites.triplet:
        import_snotel(site_triplet)

    # Arguments for db build
    args = parse_args()
    print(args)

    if args.version:
        print('snotel_to_db.py v1.0')

    for arg_path in [args.input, args.output]:
        if not Path(arg_path).is_dir():
            print('Invalid arg filepath ({args_path}), please try again.')
            sys.exit(1)

    df_dict = get_dfs(Path(args.input), verbose=args.verbose)
    df_snotel_dv = df_dict['snotel_dv']

    for df in [df_snotel_dv]:
        write_db(
            df,
            if_exists=args.exists,
            check_dups=args.check_dups,
            zip_db=args.zip,
            verbose=args.verbose
        )
