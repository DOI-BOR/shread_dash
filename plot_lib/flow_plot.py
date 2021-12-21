# -*- coding: utf-8 -*-

import datetime as dt
import pytz
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from hydroimport import import_csas_live
from database.FLOW.rfc_to_db import import_rfc
from database.FLOW.usgs_to_db import import_nwis

from database import csas_gages, usgs_gages

from plot_lib.utils import shade_forecast,screen_csas,screen_rfc,screen_usgs

def get_log_scale_dd(ymax):
    log_scale_dd = [
        {
            'active': 0,
            'showactive': True,
            'x': 0.95,
            'y': 0.9,
            'xanchor': 'right',
            'yanchor': 'top',
            'bgcolor': 'rgba(0,0,0,0)',
            'type': 'dropdown',
            'direction': 'down',
            'font': {
                'size': 10
            },
            'buttons': [
                {
                    'label': 'Linear Scale',
                    'method': 'relayout',
                    'args': ['yaxis', dict(
                                        title='Flow (ft^3/s)',
                                        side="left",
                                        type="linear",
                                        range=[1, ymax],
                                        showline=True,
                                        linecolor="black",
                                        mirror=True
                                        )
                             ]
                },
                {
                    'label': 'Log Scale',
                    'method': 'relayout',
                    'args': ['yaxis', dict(
                                        title='Flow (ft^3/s)',
                                        side="left",
                                        type="log",
                                        range=[0.1, np.ceil(np.log10(ymax))],
                                        showline=True,
                                        linecolor="black",
                                        mirror=True
                                        )
                             ]
                },
            ]
        }
    ]
    return log_scale_dd

def get_flow_plot(usgs_sel, dtype, plot_forecast, start_date, end_date, csas_sel,
                  plot_albedo,offline=True):
    """
    :description: this function updates the flow plot
    :param usgs_sel: list of selected usgs sites ([])
    :param dtype: data type (dv/iv)
    :param plot_forecast: boolean, plot forecast data (NWS-RFC)
    :param start_date: start date (from date selector)
    :param end_date: end date (from date selector)
    :param csas_sel: list of selected csas sites ([])
    :param plot_albedo: boolean, plot albedo data for selected csas_sel
    :return: update figure
    """

    # Check if forecast data needed
    if pd.to_datetime(end_date) <= dt.datetime.now():
        plot_forecast = []  # no forecast data needed if dates aren't displayed

    # Create output dfs with standard index
    if dtype == "dv":
        dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')
    elif dtype == "iv":
        dates = pd.date_range(start_date, end_date, freq="15T", tz='UTC')

    # Create dataframes for data, names and rfc sites
    usgs_f_df = pd.DataFrame(index=dates)
    name_df = pd.DataFrame(index=usgs_sel)

    if plot_forecast:
        rfc_f_df = pd.DataFrame(index=dates)

    for g in usgs_sel:
        name_df.loc[g, "usgs"] = name_df.loc[g, "name"] = f'{g} {usgs_gages.loc[int(g), "name"]}'

        if offline:
            usgs_in = screen_usgs(g,start_date,end_date,dtype)
        else:
            usgs_in = import_nwis(g,start_date,end_date,dtype)

        usgs_in = usgs_f_df.merge(usgs_in["flow"],left_index=True,right_index=True,how="left")
        usgs_f_df[g] = usgs_in["flow"]

    if plot_forecast:
        print("Attempting to include forecast data")

        # dummy forecast date for now
        fcst_dt = "last"

        for g in usgs_sel:
            if pd.isna(usgs_gages.loc[int(g),"rfc"])==False:
                rfc = usgs_gages.loc[int(g),"rfc"]
                print(rfc)

                if offline:
                    rfc_in,fcst_dt = screen_rfc(rfc,fcst_dt,dtype)
                else:
                    rfc_in,fcst_dt = import_rfc(rfc,dtype)

                usgs_interp = True
                if dtype == "dv":
                    rfc_in.index = rfc_in.index + dt.timedelta(hours=-12)
                    usgs_last = usgs_f_df[g].dropna().index.max()
                    if pd.isna(usgs_last):
                        usgs_interp = False

                rfc_in = rfc_f_df.merge(rfc_in["flow"], left_index=True, right_index=True, how="left")
                if (dtype == "dv") and (usgs_interp):
                    rfc_in.loc[usgs_last,"flow"] = usgs_f_df.loc[usgs_last,g]
                rfc_f_df[g] = rfc_in["flow"].interpolate()

                name_df.loc[g,"rfc"] = f"RFC {rfc} {fcst_dt}"
                name_df.loc[g,"name"] = f'{name_df.loc[g, "usgs"]} ({name_df.loc[g,"rfc"]})'

    if len(usgs_sel) > 0:
        flow_max = usgs_f_df.max().max()
        if (plot_forecast) and (len(rfc_f_df)>0):
            flow_max = np.nanmax([flow_max,rfc_f_df.max().max()])
    else:
        print("No FLOW selected.")
        flow_max = 50


    ## Process CSAS data (if selected)
    if len(csas_sel)>0:
        csas_f_df = pd.DataFrame()
        csas_a_df = pd.DataFrame()
        for site in csas_sel:
            if offline:
                csas_df = screen_csas(site,start_date,end_date,dtype)
            else:
                csas_df = import_csas_live(site,start_date,end_date,dtype)

            if site == "SBSG":
                csas_f_df[site] = csas_df["flow"]
            elif site != "PTSP":
                csas_a_df[site] = csas_df["albedo"]

        csas_max = np.nanmax([csas_f_df.max().max(),csas_a_df.max().max()])
    else:
        csas_max = np.nan

    ymax = np.nanmax([flow_max,csas_max]) * 1.25

    print("Updating flow plot...")

    fig = go.Figure()
    for g in usgs_sel:
        fig.add_trace(go.Scatter(
            x=usgs_f_df.index,
            y=usgs_f_df[g],
            text=name_df.loc[g, "usgs"],
            mode='lines',
            line=dict(color=usgs_gages.loc[int(g), "color"]),
            name=name_df.loc[g, "name"],
            yaxis="y1"))
        if (plot_forecast) and (g in rfc_f_df.columns):
            fig.add_trace(go.Scatter(
                x=rfc_f_df.index,
                y=rfc_f_df[g],
                text=name_df.loc[g, "rfc"],
                mode='lines',
                line=dict(color=usgs_gages.loc[int(g), "color"],dash="dash"),
                name=name_df.loc[g, "rfc"],
                showlegend=False,
                yaxis="y1"))

    if len(csas_sel) > 0:
        for c in csas_f_df.columns:
            fig.add_trace(go.Scatter(
                x=csas_f_df.index,
                y=csas_f_df[c],
                text=c+" Flow",
                mode='lines',
                line=dict(color="green",dash="dot"),
                name=c+" Flow",
                yaxis="y1"))
        if plot_albedo == True:
            for c in csas_a_df.columns:
                fig.add_trace(go.Scatter(
                    x=csas_a_df.index,
                    y=(1-csas_a_df[c])*100,
                    text="100% - Albedo",
                    mode='lines',
                    line=dict(color=csas_gages.loc[c, "color"],dash="dash"),
                    name=c+" 100% - Albedo",
                    yaxis="y2"))

    fig.add_trace(shade_forecast(1000000))

    fig.update_layout(
        margin={'l': 40, 'b': 40, 't': 0, 'r': 45},
        height=400,
        legend={'x': 0, 'y': 1, 'bgcolor': 'rgba(255,255,255,0.8)'},
        hovermode='closest',
        plot_bgcolor='white',
        xaxis=dict(
            range=[start_date, end_date],
            showline=True,
            linecolor="black",
            mirror=True
        ),
        yaxis=dict(
            title='Flow (ft^3/s)',
            side="left",
            type="linear",
            range=[1, ymax],
            showline=True,
            linecolor="black",
            mirror=True
        ))
    fig.update_layout(
        updatemenus=get_log_scale_dd(ymax)
    )
    if plot_albedo == True:
        fig.update_layout(
            yaxis2=dict(
            title="100% - Albedo",
            side="right",
            overlaying='y',
            range=[0,100]),
            margin = {'l': 40, 'b': 40, 't': 0, 'r': 40},
        )
        
    return fig
