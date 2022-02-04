# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27, 2022

SHREAD Dash Snow Plot

Script for running the snow plot in the dashboard (shread_dash.py)

@author: buriona, tclarkin (2020-2022)

"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plot_lib.utils import import_snotel,import_csas_live

from database import snotel_sites
from database import csas_gages
from plot_lib.utils import screen_spatial,ba_stats_all,ba_stats_std,screen_csas,screen_snotel
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
                     end_date, dtype,snotel_sel,csas_sel,forecast_sel,plot_albedo,
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
    if stype == "sd":
        ylabel = "Mean Snow Depth (in)"
        dlabel = "snow depth"
        slabel = "SNWD"

    ## Process SHREAD data
    # Filter data
    if basin == None:
        snodas_plot = False
        snodas_max = np.nan
        basin_stats_str = ''
    else:
        snodas_plot = True
        snodas_df = screen_spatial(
            stype, start_date, end_date, basin, aspects, elrange, slopes
        )
        if snodas_df.empty:
            snodas_plot = False
            snodas_max = np.nan
            basin_stats_str = 'No valid SHREAD data for given parameters'
        else:
            # Calculate basin average values
            ba_snodas = ba_stats_all(snodas_df)
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

    # Process NDFD, if selected

    # Filter data
    rhm = sky = snow = False

    if (basin != None) or (len(forecast_sel)>0):

        # remove rfc
        if "flow" in forecast_sel:
            forecast_sel.remove("flow")

        # check if there are still items
        if len(forecast_sel) > 0:

            if dtype=="iv":
                step="D"
            elif dtype=="dv":
                step="D"

            ndfd_max = 0
            rhm = sky = snow = False
            for sensor in forecast_sel:

                if sensor in ["qpf","maxt","mint","pop12"]:
                    continue

                df = screen_spatial(sensor,start_date,end_date,basin,aspects,elrange,slopes,"Date")
                if df.empty:
                    continue
                else:
                    # Calculate basin average values
                    ba_ndfd = ba_stats_std(df, "Date")
                    ba_ndfd = ba_ndfd.tz_localize(tz="utc")

                    if sensor!="qpf":
                        ba_ndfd = ba_ndfd['mean'].resample(step).mean()
                    else:
                        ba_ndfd = ba_ndfd['mean'].resample(step).sum()

                    ndfd = pd.DataFrame(index=dates)

                    if sensor == "sky":
                        sky = ndfd.merge(ba_ndfd,left_index=True,right_index=True,how="left")

                    if sensor == "snow":
                        snow = ndfd.merge(ba_ndfd-1,left_index=True,right_index=True,how="left")

                    if sensor == "rhm":
                        rhm = ndfd.merge(ba_ndfd, left_index=True, right_index=True, how="left")

    ### Plot the data
    ymax = np.nanmax([snodas_max,snotel_max,20]) * 1.25

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

    if snow is not False:
        fig.add_trace(go.Scatter(
            x=snow.index,
            y=[ymax - 2] * len(snow),
            mode="text",
            textfont=dict(
                color="black"
            ),
            marker=dict(color="black"),
            text=snow.round(2),
            name="Snow (in, SWE)",
            showlegend=False,
            yaxis="y1"
        ))

    if sky is not False:
        fig.add_trace(go.Scatter(
            x=sky.index,
            y=[ymax-4]*len(sky),
            mode="text",
            textfont=dict(
                color="green"
            ),
            marker=dict(color="green"),
            text=sky.round(0),
            name="Sky Coverage (%)",
            showlegend=False,
            yaxis="y1"
        ))

    if rhm is not False:
        fig.add_trace(go.Scatter(
            x=rhm.index,
            y=[ymax - 6] * len(rhm),
            mode="text",
            textfont=dict(
                color="brown"
            ),
            marker=dict(color="brown"),
            text=rhm.round(0),
            name="Relative Humidity",
            showlegend=False,
            yaxis="y1"
        ))

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

    if snodas_plot:
        return fig, basin_stats_str
    
    return fig, basin_stats_str
