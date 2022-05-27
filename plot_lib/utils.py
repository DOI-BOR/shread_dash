# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:20:37 2021

Additional utilities used in multiple scripts for shread_dash.py

@author: buriona,tclarkin
"""

import time
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import dash
from database import db
import datetime as dt
from datetime import timezone
from requests import get as r_get
from requests.exceptions import ReadTimeout
from io import StringIO

# Function for importing data
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

    failed = True
    tries = 0
    while failed:
        try:
            csv_str = r_get(site_url, timeout=None,verify=True).text
            failed = False
        except TimeoutError:
            raise Exception("Timeout; Data unavailable?")
            tries += 1
            if tries > 10:
                return

    csv_io = StringIO(csv_str)
    try:
        f = pd.read_html(csv_io)
    except ValueError:
        return

    csas_in = f[1]
    if csas_in.empty:
        return pd.DataFrame(columns=["snwd","temp","flow","albedo"])
    if csas_in is None:
        return pd.DataFrame(columns=["snwd","temp","flow","albedo"])

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

# Function to screen data by basin, aspect, elevation and slopes (using points)
def screen_spatial(db_type, s_date, e_date, basin, aspects=[0, 360],
                  elrange=[0, 20000], slopes=[0, 100],date_col="Date"):

    bind = db_type
    qry = (
        f"select * from {basin} where "
        f"`{date_col}` >= '{s_date}' "
        f"and `{date_col}` <= '{e_date}' "
        f"and slope_d >= {slopes[0]} "
        f"and slope_d <= {slopes[1]} "
        f"and elev_ft >= {elrange[0]} "
        f"and elev_ft <= {elrange[1]} "
    )
    input_df = pd.read_sql(qry, db.get_engine(bind=bind), parse_dates=['Date'])
    if aspects[0] < 0:
        minaspect = 360 + aspects[0]
        out_df = input_df[
            (input_df["aspct"] >= minaspect) | (input_df["aspct"] <= aspects[1])
        ]
    else:
        out_df = input_df[
            (input_df["aspct"] >= aspects[0]) & (input_df["aspct"] <= aspects[1])
        ]
    
    out_df.index = pd.to_datetime(out_df['Date'], utc=True)
    out_df.index.name = None
    return (out_df)

# Function to calculate mean, median, 5th and 95th states for screened basin
def ba_stats_std(df, date_field="Date"):
    ba_df = df[[date_field, 'mean']].groupby(
        by=date_field,
        sort=True,
    ).describe().droplevel(0, axis=1)
    return ba_df

def ba_stats_all(df, date_field="Date"):
    ba_df = df[[date_field, 'mean']].groupby(
        by=date_field,
        sort=True,
    ).describe(percentiles=[0.05, 0.5, 0.95]).droplevel(0, axis=1)
    return ba_df


# Function to screen csas data by site and date
def screen_csas(site,s_date,e_date,dtype):
    bind_dict = {
        'iv': 'csas_iv',
        'dv': 'csas_dv'
    }
    bind = bind_dict[dtype]
    qry = (
        f"select * from {site} where "
        f"`date` >= '{s_date}' "
        f"and `date` <= '{e_date}' "
    )
    out_df = pd.read_sql(qry, db.get_engine(bind=bind), parse_dates=['date'])

    out_df.index = pd.to_datetime(out_df['date'], utc=True)
    out_df.index.name = None
    return (out_df)

# Function to screen snotel data by site and date
def screen_snotel(site,s_date,e_date):
    bind = 'snotel_dv'
    qry = (
        f"select * from {site} where "
        f"`date` >= '{s_date}' "
        f"and `date` <= '{e_date}' "
    )
    out_df = pd.read_sql(qry, db.get_engine(bind=bind), parse_dates=['date'])

    out_df.index = pd.to_datetime(out_df['date'], utc=True)
    out_df.index.name = None
    return (out_df)

def screen_usgs(site,s_date,e_date,dtype):
    bind = f'usgs_{dtype}'
    qry = (
        f"select * from site_{site} where "
        f"`date` >= '{s_date}' "
        f"and `date` <= '{e_date}' "
    )
    out_df = pd.read_sql(qry, db.get_engine(bind=bind), parse_dates=['date'])

    out_df.index = pd.to_datetime(out_df['date'], utc=True)
    out_df.index.name = None
    return (out_df)

def screen_rfc(site,fcst_dt,dtype):
    bind = f'rfc_{dtype}'

    # Check for last forecast date
    if fcst_dt=="last":
        unique_dates = pd.read_sql(
            f'select distinct fcst_dt from site_{site}',
            db.get_engine(bind=bind),parse_dates=['fcst_dt']
        ).dropna()
        last = unique_dates.max().item()
        fcst_dt = last.strftime("%Y-%m-%d")

    qry = (
        f"select * from site_{site} where "
        f"`fcst_dt` = '{fcst_dt}' "
    )
    out_df = pd.read_sql(qry, db.get_engine(bind=bind), parse_dates=['date'])

    out_df.index = pd.to_datetime(out_df['date'], utc=True)
    out_df.index.name = None
    return (out_df,fcst_dt)

# Functions to plot max, min, mean and median ba timeseries
def ba_max_plot(ba_df,dlabel,color="grey"):
    return(go.Scatter(
        x=ba_df.index,
        y=ba_df["95%"],
        text=dlabel,
        mode='lines',
        opacity=0.5,
        fill="tozeroy",
        fillcolor="light grey",
        line=dict(color=color),
        showlegend=False,
        name="95% " + dlabel + " for selection"))

def ba_min_plot(ba_df, dlabel,color="grey"):
    return(go.Scatter(
        x=ba_df.index,
        y=ba_df["5%"],
        text=dlabel,
        mode='lines',
        opacity=0,
        fill="tozeroy",
        fillcolor="white",
        line=dict(color=color),
        showlegend=False,
        name="5% " + dlabel + " for selection"))

def ba_mean_plot(ba_df,dlabel,color="black"):
    return(go.Scatter(
        x=ba_df.index,
        y=ba_df["mean"],
        text=dlabel,
        mode='lines',
        line=dict(color=color),
        name="Mean " + dlabel + " for selection"))

def ba_median_plot(ba_df,dlabel,color="black"):
    return(go.Scatter(
        x=ba_df.index,
        y=ba_df["50%"],
        text=dlabel,
        mode='lines',
        line=dict(color=color, dash="dash"),
        showlegend=False,
        name="Median " + dlabel + " for selection"))

# Function to plot forecast zone on figures
def shade_forecast(ymax):
    return (go.Scatter(
        x=[dt.datetime.utcnow(), dt.datetime.now().date() + dt.timedelta(days=10)],
        y=[ymax, ymax],
        text="Forecast Data",
        showlegend=False,
        mode='lines',
        fill="tozeroy",
        fillcolor="rgba(255,0,0,0.10)",
        line=dict(color="rgba(255,0,0,0.10)"),
        name="Forecast Data",
        yaxis="y1"
    ))

def get_plot_config(img_filename):
    return {
    'modeBarButtonsToRemove': [
    'sendDataToCloud',
    'lasso2d',
    'select2d'
    ],
    'showAxisDragHandles': True,
    'showAxisRangeEntryBoxes': True,
    'displaylogo': False,
    'toImageButtonOptions': {
        'filename': img_filename,
        'width': 1200,
        'height': 700
        }
    }
##
# came from: https://stackoverflow.com/questions/49371248/plotly-dash-has-unknown-issue-and-creates-error-loading-dependencies-by-using/51003812#51003812
def unixTimeMillis(dt):
    ''' Convert datetime to unix timestamp '''
    return int(time.mktime(dt.timetuple()))

def unixToDatetime(unix):
    ''' Convert unix timestamp to datetime. '''
    return pd.to_datetime(unix,unit='s')

def getMarks(start, end, Nth=24):
    ''' Returns the marks for labeling.
        Every Nth value will be used.
    '''

    result = {}
    for i, date in enumerate(pd.date_range(start,end,freq="H")):
        if(i%Nth == 1):
            # Append value to dict
            result[unixTimeMillis(date)] = str(date.strftime('%m-%d'))

    return result
