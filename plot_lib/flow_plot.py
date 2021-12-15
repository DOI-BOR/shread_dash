# -*- coding: utf-8 -*-

import datetime as dt
import pytz
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from hydroimport import import_rfc,import_csas_live,nwis_import
import dataretrieval.nwis as nwis

from database import csas_gages, usgs_gages

from plot_lib.utils import shade_forecast,screen_csas

def get_log_scale_dd(ymax):
    log_scale_dd = [
        {
            'active': 0,
            'showactive': True,
            'x': 1,
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
    #print(plot_albedo)
    #print(plot_forecast)
    #Based on data type (daily or instantaneous), flow data date index
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

    if plot_forecast == True:
        print("Attempting to include forecast data")
        if dtype == "dv":
            forecast_begin = pytz.timezone("UTC").localize(dt.datetime.today())
        if dtype == "iv":
            forecast_begin = pytz.timezone("America/Denver").localize(dt.datetime.now())

        for g in usgs_sel:
            if rfc_f_df.name[g] != "nan":
                rfc = rfc_f_df.name[g]
                name_df.name[g] = name_df.name[g] + " (RFC: " + rfc + ")"
                flow_in = import_rfc(rfc, dtype)
                flow_in.loc[flow_in["FLOW"] < 0, "FLOW"] = np.nan

                if dtype == "dv":
                    flow_in.index = flow_in.index + dt.timedelta(hours=-12)
                if dtype == "iv":
                    flow_in = flow_in.tz_convert("America/Denver")
                # print(flow_in)
                flow_in = usgs_f_df.merge(flow_in["FLOW"], left_index=True, right_index=True, how="left")
                flow_in = flow_in.fillna(method="ffill")
                # print(flow_in)
                usgs_f_df.loc[usgs_f_df.index >= forecast_begin, g] = flow_in.loc[flow_in.index >= forecast_begin, "FLOW"]

    if len(usgs_f_df.columns) == 0:
        print("No USGS selected.")
        usgs_f_max = 50
    else:
        usgs_f_max = usgs_f_df.max().max()


    ## Process CSAS data (if selected)
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
    ymax = np.nanmax([usgs_f_max,csas_max]) * 1.25

    print("Updating flow plot...")

    fig = go.Figure()
    for g in usgs_sel:
        fig.add_trace(go.Scatter(
            x=usgs_f_df.index,
            y=usgs_f_df[g],
            text=name_df.loc[g, "name"],
            mode='lines',
            line=dict(color=usgs_gages.loc[int(g), "color"]),
            name=name_df.loc[g, "name"],
            yaxis="y1"))

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
