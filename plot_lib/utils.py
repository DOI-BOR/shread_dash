# -*- coding: utf-8 -*-

import time
import datetime as dt
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from database import db

# Function to screen data by basin, aspect, elevation and slopes (using points)
def screen_snodas(db_type, s_date, e_date, basin, aspects=[0, 360], 
                  elrange=[0, 20000], slopes=[0, 100]):

    bind_dict = {
        'swe': 'swe',
        'snowdepth': 'sd'
    }
    bind = bind_dict[db_type]
    qry = (
        f"select * from {basin} where "
        f"`Date` >= '{s_date}' "
        f"and `Date` <= '{e_date}' "
        f"and slope_d >= {slopes[0]} "
        f"and slope_d <= {slopes[1]} "
        f"and elev_ft >= {elrange[0]} "
        f"and elev_ft <= {elrange[1]} "
    )
    # print(db_type, bind, qry)
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

# Function to screen data by basin, aspect, elevation and slopes (using points)
def screen(input_df, basin, aspects=[0, 360], elrange=[0, 20000], slopes=[0, 100]):
    """
    # Function to screen gridded/point datasets
    :param input_df: the input dataframe
    :param basin:
    :param aspects:
    :param elrange:
    :param slopes:
    :return:
    """
    if aspects[0] < 0:
        minaspect = 360 + aspects[0]
        out_df = input_df[
            (input_df["aspct"] >= minaspect) | (input_df["aspct"] <= aspects[1])
        ]
    else:
        out_df = input_df[
            (input_df["aspct"] >= aspects[0]) & (input_df["aspct"] <= aspects[1])
        ]

    out_df = out_df[(out_df["elev_ft"] >= elrange[0]) &
                    (out_df["elev_ft"] <= elrange[1]) &
                    (out_df["slope_d"] >= slopes[0]) &
                    (out_df["slope_d"] <= slopes[1]) &
                    (out_df["LOCAL_ID"].isin(basin))]
    return (out_df)

# Function to calculate mean, median, 5th and 95th states for screened basin
def ba_stats(df, dates):
    ba_df = df[['Date', 'mean']].groupby(
        by='Date',
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
    # print(db_type, bind, qry)
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
    # print(db_type, bind, qry)
    out_df = pd.read_sql(qry, db.get_engine(bind=bind), parse_dates=['date'])

    out_df.index = pd.to_datetime(out_df['date'], utc=True)
    out_df.index.name = None
    return (out_df)

# Function to calculate mean, median, 5th and 95th states for screened basin
def ba_snodas_stats(df, dates):
    
    ba_df = df[['Date', 'mean']].groupby(
        by='Date',
        sort=True,
    ).describe(percentiles=[0.05, 0.5, 0.95]).droplevel(0, axis=1)

    return ba_df

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

