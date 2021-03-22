# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import hydroimport as hydro

from database import snotel_gages, SBSP_iv, SBSP_dv, SASP_iv, SASP_dv
from database import csas_gages, dust_ts, dust_layers
from plot_lib.utils import screen_snodas, ba_snodas_stats
from plot_lib.utils import ba_min_plot, ba_max_plot, ba_mean_plot, ba_median_plot
from plot_lib.utils import plot_forecast

def get_basin_stats(snodas_df):
    
    snodas_unique = snodas_df.drop_duplicates("OBJECTID")
    mean_el = round(snodas_unique["elev_ft"].mean(),0)
    points = len(snodas_unique)
    area = round(points * 0.386102, 0)
    stats = (
        f'Mean Elevation: {mean_el} feet & Area: {area} sq.mi. '
        f'(approximated by {points} points)'
    )
    return stats

def get_snow_plot(basin, stype, elrange, aspects, slopes, start_date, 
                     end_date, snotel_sel, plot_dust, csas_sel, dtype):
    """
    :description: this function updates the snowplot
    :param basin: the selected basins (checklist)
    :param stype: the snow type (swe/snowdepth)
    :param elrange: the range of elevations ([min,max])
    :param aspects: the range of aspects  ([min,max])
    :param slopes: the range of slopes ([min,max])
    :param start_date: start date (from date selector)
    :param end_date: end date (from date selector)
    :param snotel_sel: list of selected snotel sites ([])
    :param plot_dust: boolean for plotting dust layers
    :param csas_sel: list of selected csas sites ([])
    :param dtype: data type (dv/iv)
    :return: update figure
    """
    
    # Create date axis
    dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')

    # Set snow type based on user selection
    if stype == "swe":
        # snodas = snodas_swe
        ylabel = "Mean SWE (in)"
        dlabel = "SWE"
        slabel = "WTEQ"
        plot_dust = False
        csas_sel = []
    if stype == "snowdepth":
        # snodas = snodas_sd
        ylabel = "Mean Snow Depth (in)"
        dlabel = "snow depth"
        slabel = "SNWD"
        cvar = "Sno_Height_M"

    ## Process SNODAS data
    # Filter data
    if basin == []:
        print("No basins selected.")
        snodas_plot = False
        snodas_max = np.nan
    else:
        snodas_plot = True
        snodas_df = screen_snodas(
            stype, start_date, end_date, basin, aspects, elrange, slopes
        )
        # Calculate basin average values
        ba_snodas = ba_snodas_stats(snodas_df, dates)
        snodas_max = ba_snodas['95%'].max()
    ## Process SNOTEL data (if selected)

    # Add data for selected SNOTEL sites
    snotel_s_df = pd.DataFrame(index=dates)
    name_df = pd.DataFrame(index=snotel_sel)
    for s in snotel_sel:
        name_df.loc[s, "name"] = str(snotel_gages.loc[s, "site_no"]) + " " + snotel_gages.loc[s, "name"] + " (" + str(
            round(snotel_gages.loc[s, "elev_ft"], 0)) + " ft)"
        snotel_in = hydro.import_snotel(s, start_date, end_date, vars=[slabel])
        snotel_in = snotel_s_df.merge(snotel_in[slabel], left_index=True, right_index=True, how="left")
        snotel_s_df.loc[:, s] = snotel_in[slabel]

    if len(snotel_sel) == 0:
        snotel_max = np.nan
        print("No SNOTEL selected.")
    else:
        snotel_max = snotel_s_df.max().max()

    ## Process CSAS data (if selected)
    if len(csas_sel)>0:
        for sp in ["PTSP","SBSG"]:
            if sp in csas_sel:
                csas_sel.remove(sp)

    if dtype=="dv":
        cdates = dates
    if dtype=="iv":
        cdates = pd.date_range(start_date, end_date, freq="H", tz='UTC')

    csas_s_df = pd.DataFrame(index=cdates)

    for c in csas_sel:

        if c=="SASP":
            if dtype=="dv":
                csas_in = SASP_dv
            if dtype=="iv":
                csas_in = SASP_iv
        if c=="SBSP":
            if dtype=="dv":
                csas_in = SBSP_dv
            if dtype=="iv":
                csas_in = SBSP_iv

        csas_in = csas_in[(csas_in.index>=start_date) & (csas_in.index<=end_date)]
        csas_in = csas_s_df.merge(csas_in[cvar], left_index=True, right_index=True, how="left")
        csas_s_df.loc[:, c] = csas_in[cvar]*3.28*12

    if len(csas_sel) == 0:
        csas_max = np.nan
        print("No CSAS selected.")
    else:
        csas_max = csas_s_df.max().max()

    ### Plot the data
    ymax = max([snodas_max, snotel_max, csas_max,20]) * 1.25

    print("Updating snow plot...")
    fig = go.Figure()

    if snodas_plot==True:
        fig.add_trace(ba_max_plot(ba_snodas, dlabel))
        fig.add_trace(ba_min_plot(ba_snodas, dlabel))
        fig.add_trace(ba_mean_plot(ba_snodas, dlabel))
        fig.add_trace(ba_median_plot(ba_snodas, dlabel))

    for s in snotel_sel:
        fig.add_trace(go.Scatter(
            x=snotel_s_df.index,
            y=snotel_s_df[s],
            text=ylabel,
            mode='lines',
            line=dict(color=snotel_gages.loc[s, "color"]),
            name=name_df.loc[s, "name"]))

    for c in csas_sel:
        fig.add_trace(go.Scatter(
            x=csas_s_df.index,
            y=csas_s_df[c],
            text=ylabel,
            mode='lines',
            line=dict(color=csas_gages.loc[c, "color"],dash="dot"),
            name=c))

    if plot_dust == True:
        for d in dust_ts.columns:
            fig.add_trace(go.Scatter(
                x=dust_ts.index,
                y=dust_ts[d],
                text=ylabel,
                mode='lines+markers',
                line=dict(color=dust_layers.loc[d, "color"],dash="dot"),
                name=d))

    fig.add_trace(plot_forecast(ymax))
    fig.update_layout(
        xaxis={'range': [start_date, end_date]},
        yaxis={'title': ylabel, 'type': 'linear', 'range': [0, ymax]},
        margin={'l': 40, 'b': 40, 't': 10, 'r': 45},
        height=250,
        legend={'x': 0, 'y': 1, 'bgcolor': 'rgba(0,0,0,0)'},
        hovermode='closest',
        plot_bgcolor="white"
    )
    print('snow plot is done')
    
    if snodas_plot:
        return fig, get_basin_stats(snodas_df)
    
    return fig, ''