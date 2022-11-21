# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

Compiles USGS data into SQLite DB

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
#this_dir = Path(__file__).absolute().resolve().parent
this_dir = Path('C:/Programs/shread_dash/database/FLOW')
ZIP_IT = False
ZIP_FRMT = zipfile.ZIP_LZMA
DEFAULT_DATE_FIELD = 'date'
DEFAULT_CSV_DIR = Path(this_dir,'usgs_data')
DEFAULT_DB_DIR = this_dir

# TODO check this!
COL_TYPES = {
    'date': str,'flow':float,'site':str,'type':str
}

# Define functions
def import_nwis(site,start=None,end=None,dtype="dv",data_dir=None):
    """
    Imports flows from NWIS site
    :param site: str, FLOW site number
    :param dtype: str, "dv" or "iv"
    :param start: str, start date (default is None)
    :param end: str, end date (default is None)
    :return: dataframe with date index, dates, flows, month, year and water year
    """
    # Output directory
    if data_dir is not None:
        if os.path.isdir(data_dir) is False:
            os.mkdir(data_dir)

    # Correct dtype and dates
    if (end is None) or (pd.to_datetime(end) >= dt.datetime.now()):
        enddt = dt.datetime.now()
        end = enddt.strftime("%Y-%m-%d")
        currentyear = int(end[:4])

    if dtype == "dv":
        parameter = "00060_Mean"
        if start is None:
            start = f"{currentyear-1}-10-01"
        nd_start = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S+00:00")
    elif dtype == "iv":
        parameter = "00060"
        if start is None:
            if (enddt > dt.datetime.strptime(f"{currentyear}-11-07","%Y-%m-%d")):
                start = f"{currentyear}-11-07"
            elif (enddt < dt.datetime.strptime(f"{currentyear}-03-08","%Y-%m-%d")):
                start = f"{currentyear-1}-11-07"
            elif (enddt > dt.datetime.strptime(f"{currentyear}-03-14","%Y-%m-%d")) and (enddt < dt.datetime.strptime(f"{currentyear}-11-01","%Y-%m-%d")):
                start = f"{currentyear}-03-14"
            else:
                start = end

        nd_start = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S-06:00")



    # Import data
    try:
        data = nwis.get_record(sites=site, start=start, end=end, service=dtype, parameterCd="00060")
    except ValueError:
        data = pd.DataFrame(columns=COL_TYPES.keys())
        data.loc[0,:] = [nd_start,np.nan,site,f"usgs_{dtype}"]
        if data_dir is None:
            return(data)
        else:
            data.to_csv(Path(data_dir,f"{site}_{dtype}.csv"),index=False)
            return

    if data.empty:
        data = pd.DataFrame(columns=COL_TYPES.keys())
        data.loc[0,:] = [nd_start,np.nan,site,f"usgs_{dtype}"]
        if data_dir is None:
            return(data)
        else:
            data.to_csv(Path(data_dir,f"{site}_{dtype}.csv"),index=False)
            return

    # Prepare output with standard index
    #if dtype=="iv":
    #    data.index = pd.to_datetime(data.index,utc=True)

    start_date = data.index.min()
    end_date = data.index.max()

    if dtype == "dv":
        date_index = pd.date_range(start_date, end_date, freq="D")
    elif dtype == "iv":
        date_index = pd.date_range(start_date, end_date,freq="15T")

    out = pd.DataFrame(index=date_index)
    out["flow"] = out.merge(data[parameter], left_index=True, right_index=True, how="left")

    # Correct errors
    out.loc[out["flow"]<0,"flow"] = np.nan

    if data_dir is None:
        return(out)
    else:
        out["site"] = site
        out["type"] = f"usgs_{dtype}"
        out.to_csv(Path(data_dir,f"{site}_{dtype}.csv"), index_label="date")

def get_dfs(data_dir=DEFAULT_CSV_DIR,verbose=False):
    """
    Get and merge dataframes imported using functions
    """
    usgs_df_dv_list = []
    usgs_df_iv_list = []
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
            usgs_df_iv_list.append(
                df[df['type'] == 'usgs_iv'].drop(columns='type').copy()
            )
            usgs_df_dv_list.append(
                df[df['type'] == 'usgs_dv'].drop(columns='type').copy()
            )

    df_usgs_dv = pd.concat(usgs_df_dv_list)
    df_usgs_dv.name = 'usgs_dv'
    df_usgs_iv = pd.concat(usgs_df_iv_list)
    df_usgs_iv.name = 'usgs_iv'
    print('  Success!!!\n')
    return {'usgs_dv':df_usgs_dv,
            'usgs_iv':df_usgs_iv}

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
        site = str(site_no)
        if len(site)<8:
            site = f"0{site}"
        print(f"Downloading data for {site}")

        # if "end" in str(usgs_sites.columns):
        #     if usgs_sites.loc[site_no,"end"]==None:
        #         start = None
        #         usgs_sites.loc[site_no, "end"] = dt.datetime.now().strftime("%Y-%m-%d")
        #     else:
        #         start = usgs_sites.loc[site_no,"end"]
        # else:
        #     start = None
        #     usgs_sites.loc[:,"end"] = None
        #     usgs_sites.loc[site_no, "end"] = dt.datetime.now().strftime("%Y-%m-%d")
        #
        # end = dt.datetime.now().strftime("%Y-%m-%d")

        for dtype in ["dv", "iv"]:
            #if (start!=end):
            import_nwis(site,None,None,dtype,DEFAULT_CSV_DIR)

    #usgs_sites.to_csv(os.path.join(this_dir, "usgs_gages.csv"),index_label="site_no")

    #TODO: fix duplicate issues

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
