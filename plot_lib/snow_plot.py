# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from hydroimport import import_snotel,import_csas_live

from database import snotel_sites
from database import csas_gages
from plot_lib.utils import screen_snodas,ba_snodas_stats,screen_csas,screen_snotel
from plot_lib.utils import ba_min_plot, ba_max_plot, ba_mean_plot, ba_median_plot
from plot_lib.utils import shade_forecast

def get_basin_stats(snodas_df,stype="swe"):
    dates = snodas_df["Date"].unique()
    last_date = dates.max()
    snodas_unique = snodas_df[snodas_df["Date"]==last_date]
    mean_el = round(snodas_unique["elev_ft"].mean(),0)
    points = len(snodas_unique)
    area = round(points * 0.386102, 0)

    if stype=="swe":
        mean_ft = snodas_unique["mean"].mean()/12
        vol_af = round(mean_ft*area*640,0)
        stats = (
            f'Volume: ~{vol_af:,.0f} acre-feet | '
            f'Mean Elevation: {mean_el:,.0f} feet & Area: {area:,.0f} sq.mi. | '
            f'(approximated by {points} points)'
        )
    else:
        stats = (
            f'Mean Elevation: {mean_el:,.0f} feet & Area: {area:,.0f} sq.mi. |'
            f'(approximated by {points} points)'
        )

    return stats

def get_snow_plot(basin, stype, elrange, aspects, slopes, start_date,
                     end_date, snotel_sel,csas_sel,plot_albedo,
                  offline=True):
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
    :param albedo: boolean
    :return: update figure
    """
    # Set dtype:
    dtype = "dv"

    # Create date axis
    dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')

    # Set snow type based on user selection
    if stype == "swe":
        ylabel = "Mean SWE (in)"
        dlabel = "SWE"
        slabel = "WTEQ"
    if stype == "snowdepth":
        ylabel = "Mean Snow Depth (in)"
        dlabel = "snow depth"
        slabel = "SNWD"

    ## Process SHREAD data
    # Filter data
    if basin == None:
        print("No basins selected.")
        snodas_plot = False
        snodas_max = np.nan
        basin_stats_str = ''
    else:
        snodas_plot = True
        snodas_df = screen_snodas(
            stype, start_date, end_date, basin, aspects, elrange, slopes
        )
        if snodas_df.empty:
            snodas_plot = False
            snodas_max = np.nan
            basin_stats_str = 'No valid SHREAD data for given parameters'
        else:
            # Calculate basin average values
            ba_snodas = ba_snodas_stats(snodas_df, dates)
            snodas_max = ba_snodas['95%'].max()
            basin_stats_str = get_basin_stats(snodas_df,stype)
            
    ## Process SNOTEL data (if selected)

    # Add data for selected SNOTEL sites
    snotel_s_df = pd.DataFrame(index=dates)
    name_df = pd.DataFrame(index=snotel_sel)
    for s in snotel_sel:
        name_df.loc[s, "name"] = str(snotel_sites.loc[s, "site_no"]) + " " + snotel_sites.loc[s, "name"] + " (" + str(
            round(snotel_sites.loc[s, "elev_ft"], 0)) + " ft)"
        if offline:
            snotel_in = screen_snotel(f"snotel_{s}", start_date, end_date)
        else:
            snotel_in = import_snotel(s, start_date, end_date, vars=[slabel])
        snotel_in = snotel_s_df.merge(snotel_in[slabel], left_index=True, right_index=True, how="left")
        snotel_s_df.loc[:, s] = snotel_in[slabel]

    if len(snotel_sel) == 0:
        snotel_max = np.nan
        print("No SNOTEL selected.")
    else:
        snotel_max = snotel_s_df.max().max()

    ## Process CSAS data (if selected)
    csas_a_df = pd.DataFrame()
    for site in csas_sel:
        if offline:
            csas_df = screen_csas(site, start_date, end_date,dtype)
        else:
            csas_df = import_csas_live(site,start_date,end_date,dtype)

        if (plot_albedo) and (site != "SBSG") and (site != "PTSP"):
            csas_a_df[site] = csas_df["albedo"]

    ### Plot the data
    ymax = max([snodas_max,snotel_max,20]) * 1.25

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
            line=dict(color=snotel_sites.loc[s, "color"]),
            name=name_df.loc[s, "name"]))

    if (plot_albedo) and (offline):
        for c in csas_a_df.columns:
            fig.add_trace(go.Scatter(
                x=csas_a_df.index,
                y=(1-csas_a_df[c])*100,
                text="100% - Albedo",
                mode='lines',
                line=dict(color=csas_gages.loc[c, "color"], dash="dash"),
                name=c + " 100% - Albedo",
                yaxis="y2"))

    fig.add_trace(shade_forecast(ymax))
    fig.update_layout(
        xaxis=dict(
            range=[start_date, end_date],
            showline=True,
            linecolor="black",
            mirror=True
        ),
        yaxis=dict(
            title = ylabel,
            type = 'linear',
            range = [0, ymax],
            showline = True,
            linecolor = "black",
            mirror = True
        ),
        margin={'l': 40, 'b': 40, 't': 10, 'r': 45},
        height=400,
        legend={'x': 0, 'y': 1, 'bgcolor': 'rgba(255,255,255,0.8)'},
        hovermode='closest',
        plot_bgcolor='white',
    )
    if (plot_albedo) and (offline):
        fig.update_layout(
            yaxis2=dict(
                title="100% - Albedo",
                side="right",
                overlaying='y',
                range=[0, 100]),
            margin={'l': 40, 'b': 40, 't': 0, 'r': 40},
        )
    print('snow plot is done')
    
    if snodas_plot:
        return fig, basin_stats_str
    
    return fig, basin_stats_str
