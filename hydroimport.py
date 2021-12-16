"""
Created on Wed Jan 15 16:35:45 2021

@author: tclarkin
"""
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timezone
from requests import get as r_get
from requests.exceptions import ReadTimeout
from io import StringIO
import dataretrieval.nwis as nwis


def import_rfc(site, dtype, rfc = "cbrfc", verbose=False):
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

    return(rfc_dat)

def import_snotel(site_triplet, start_date, end_date, vars=["WTEQ","SNWD","PREC","TAVG"],dtype="dv",verbose=False):
    """Download NRCS SNOTEL data

    Parameters
    ---------
        site_triplet: three part SNOTEL triplet (e.g., 713_CO_SNTL)
        start_date: datetime
        end_date: datetime
        vars: array of variables for import (tested with WTEQ, SNWD, PREC, TAVG..other options may be available)
        dtype: str (only daily, dv, supported)
        verbose: boolean
            True : enable print during function run

    Returns
    -------
        dataframe

    """
    # Set filepath extension for dtype
    if dtype == "dv":
        ext = "DAILY"
    if dtype == "iv":
        ext = "DAILY"
        print("Sub-daily data not available.")

    # TODO: basinaverage data
    # TODO: soil moisture

    # Create output index and dataframe
    dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')
    data = pd.DataFrame(index=dates)

    # Cycle through variables
    for var in vars:
        if verbose==True:
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
                feb29 = dt.datetime(year=year, month=3, day=1,tzinfo=timezone.utc)
                try:
                    feb29idx = json_index.get_loc(feb29)
                    if feb29idx == 0:
                        continue
                    else:
                        json_data = json_data.drop(feb29idx)
                        if verbose==True:
                            print("February 29th data for {} removed.".format(year))
                except KeyError:
                    continue

        # Concatenate the cleaned data to the date index
        snotel_in = pd.DataFrame(index=json_index)
        snotel_in[var] = json_data.values

        # For precip, calculate incremental precip and remove negative values
        if var == "PREC":
            if verbose==True:
                print("Calculating incremental Precip.")
            snotel_in["PREC"] = snotel_in[var]-snotel_in[var].shift(1)
            snotel_in.loc[snotel_in["PREC"]<0,"PREC"] = 0

        # Merge to output dataframe
        snotel_in = data.merge(snotel_in[var], left_index=True, right_index=True, how="left")
        data[var] = snotel_in[var]
        if verbose==True:
            print("Added to dataframe")

    # print(site_triplet)

    # Return output dataframe
    return(data)

def compose_date(years, months=1, days=1, weeks=None, hours=None, minutes=None,
                 seconds=None, milliseconds=None, microseconds=None, nanoseconds=None):
    years = np.asarray(years) - 1970
    months = np.asarray(months) - 1
    days = np.asarray(days) - 1
    types = ('<M8[Y]', '<m8[M]', '<m8[D]', '<m8[W]', '<m8[h]',
             '<m8[m]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]')
    vals = (years, months, days, weeks, hours, minutes, seconds,
            milliseconds, microseconds, nanoseconds)
    return sum(np.asarray(v, dtype=t) for t, v in zip(types, vals)
               if v is not None)

def import_csas_live(site, start_date, end_date,dtype="dv",verbose=False):
    """Download CSAS  data

    Parameters
    ---------
        site: four letter site ID (e.g., SBSP,SASP,PTSP,SBSG)
        start_date: datetime
        end_date: datetime
        dtype: str (only daily, dv, supported)
        verbose: boolean
            True : enable print during function run

    Returns
    -------
        dataframe
    """
    # Set filepath extension for dtype

    if dtype == "iv":
        ext = "Hourly.php"
    if dtype == "dv":
        ext = "Daily.php"

    site_url = "https://www.snowstudies.info/NRTData/" + site + "Full" + ext
    print(site_url)

    failed = True
    tries = 0
    while failed:
        try:
            csv_str = r_get(site_url, timeout=None,verify=True).text
            failed = False
        except TimeoutError:
            raise Exception("Timeout; Data unavailable?")
            tries += 1
            print(tries)
            if tries > 10:
                return

    csv_io = StringIO(csv_str)
    try:
        f = pd.read_html(csv_io)
    except ValueError:
        return

    csas_in = f[1]
    if csas_in.empty:
        print("Data not available")
        return
    if csas_in is None:
        print("Data not available")
        return

    if dtype == "dv":
        dates = compose_date(years=csas_in.Year,days=csas_in.Day)
    if dtype == "iv":
        dates = compose_date(years=csas_in.Year, days=csas_in.Day,hours=csas_in.Hour/100)

    csas_df = pd.DataFrame()

    for col in csas_in.columns:
        if "Snow Depth" in col:
            csas_df["snwd"] = csas_in[col]*3.28084*12
        if ("Daily Average Air Temperature" in col) & ("(C" in col):
            csas_df["temp"] = csas_in[col]*9/5+32
        else:
            if ("Air Temperature" in col) & ("(C" in col):
                csas_df["temp"] = csas_in[col]*9/5+32
        if "Solar Radiation-Up" in col:
            csas_df["radup"] = csas_in[col]
        if "Solar Radiation-Down" in col:
            csas_df["raddn"] = csas_in[col]
        if ("Discharge" in col) or ("discharge" in col):
            csas_df["flow"] = csas_in[col]

    # Add date index
    csas_df.index=dates

    # Calculated Albedo (if not included)
    if "radup" in csas_df.columns:
        csas_df["albedo"] = csas_df["raddn"] / csas_df["radup"]
        csas_df.loc[csas_df["albedo"]>1,'albedo'] = 1
        csas_df.loc[csas_df["albedo"]<0,"albedo"] = 0

    # Clean snow depth
    if "snwd" in csas_df.columns:
        #csas_df.loc[csas_df["snwd"]==1,"snwd"] = np.nan
        csas_df.loc[csas_df["snwd"]>109,'snwd'] = np.nan # 109 is common error value
        csas_df["snwd"] = csas_df["snwd"].interpolate(limit=3)

    if dtype == "dv":
        csas_out = csas_df.loc[(csas_df.index >= start_date) & (csas_df.index <= end_date)]
        csas_out = csas_out.tz_localize("UTC")
    if dtype == "iv":
        csas_out = csas_df.loc[(csas_df.index >= dt.datetime.strptime(start_date,"%Y-%m-%d")) & (csas_df.index <= dt.datetime.strptime(end_date,"%Y-%m-%d"))]
        csas_out = csas_out.tz_localize("America/Denver")

    return(csas_out)

def import_nwis(site, dtype, start=None, end=None):
    """
    Imports flows from NWIS site
    :param site: str, USGS site number
    :param dtype: str, "dv" or "iv"
    :param start: str, start date (default is None)
    :param end: str, end date (default is None)
    :return: dataframe with date index, dates, flows, month, year and water year
    """
    # Correct dtype
    if dtype == "dv":
        parameter = "00060_Mean"
    elif dtype == "iv":
        parameter = "00060"

    # Correct dates
    if start is None:
        start = "2004-01-01"
    if end is None:
        end = dt.datetime.now().strftime("%Y-%m-%d")
    elif pd.to_datetime(end) >= dt.datetime.now():
        end = dt.datetime.now().strftime("%Y-%m-%d")

    # Import data
    data = pd.DataFrame()
    try:
        data = nwis.get_record(sites=site, start=start, end=end, service=dtype, parameterCd='00060')
    except ValueError:
        data["flow"] = np.nan

    # Prepare output with standard index
    start = data.index.min()
    end = data.index.max()

    if dtype == "dv":
        date_index = pd.date_range(start, end, freq="D")
    elif dtype == "iv":
        date_index = pd.date_range(start, end, freq="15T")

    out = pd.DataFrame(index=date_index)
    out["flow"] = out.merge(data[parameter], left_index=True, right_index=True, how="left")

    # Correct errors
    out.loc[out["flow"]<0,"flow"] = np.nan

    return(out)