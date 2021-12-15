# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from hydroimport import import_snotel

from database import snotel_sites
from database import csas_gages

from plot_lib.utils import screen,ba_stats,screen_csas,screen_snotel
from plot_lib.utils import ba_mean_plot, ba_median_plot
from plot_lib.utils import shade_forecast

def get_met_plot(basin, elrange, aspects, slopes, start_date,
                 end_date, snotel_sel, csas_sel, plot_albedo, dtype,offline=True):
    """
    :description: this function updates the meteorology plot
    :param basin: the selected basins (checklist)
    :param elrange: the range of elevations ([min,max])
    :param aspects: the range of aspects  ([min,max])
    :param slopes: the range of slopes ([min,max])
    :param start_date: start date (from date selector)
    :param end_date: end date (from date selector)
    :param snotel_sel: list of selected snotel sites ([])
    :param csas_sel: list of selected csas sites ([])
    :param plot_albedo: boolean, plot albedo data for selected csas_sel
    :param dtype: data type (dv/iv)
    :return: update figure
    """
    print(plot_albedo)
   # Create date axis
    dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')
    freeze = 32

    ## Process NWS Basin Data (INCOMPLETE)
    # NWS Temp
    ylabel = "Avg. Temp (F)"
    nws_t_df = pd.DataFrame(index=dates)
    nws_t_df["mean"] = np.nan
    nws_t_max = nws_t_df.max().max()
    nws_t_min = nws_t_df.min().min()
    print("NWS Temp not yet added")

    # NWS Precip
    nws_p_df = pd.DataFrame(index=dates)
    nws_p_df["mean"] = np.nan
    nws_p_max = nws_p_df.max().max()
    ylabel2 = "Inc. Precip (in)"
    print("NWS Precip not yet added")

    ## Process SNOTEL data (if selected)
    if len(snotel_sel) > 0:

        # Process daily temperature and precip, create name list
        snotel_p_df = pd.DataFrame(index=dates)
        snotel_t_df = pd.DataFrame(index=dates)
        name_df = pd.DataFrame(index=snotel_sel)

        for s in snotel_sel:
            # Add name to name_df
            name_df.loc[s, "name"] = str(snotel_sites.loc[s, "site_no"]) + " " + snotel_sites.loc[
                s, "name"] + " (" + str(round(snotel_sites.loc[s, "elev_ft"], 0)) + " ft)"

            # Import SNOTEL data
            if offline:
                snotel_in = screen_snotel(f"snotel_{s}", start_date, end_date)
            else:
                snotel_in = import_snotel(s, start_date, end_date, vars=["TAVG", "PREC"])

            # Merge and add to temp and precip df
            snotel_t_in = snotel_t_df.merge(snotel_in["TAVG"], left_index=True, right_index=True, how="left")
            snotel_t_df.loc[:, s] = snotel_t_in["TAVG"]
            snotel_p_in = snotel_p_df.merge(snotel_in["PREC"], left_index=True, right_index=True, how="left")
            snotel_p_df.loc[:, s] = snotel_p_in["PREC"]

        # Calculate maximum values (for plotting axes)
        snotel_t_max = snotel_t_df.max().max()
        snotel_t_min = snotel_t_df.min().min()
        snotel_p_max = snotel_p_df.max().max()

    else:
        snotel_t_max = np.nan
        snotel_t_min = np.nan
        snotel_p_max = np.nan
        print("No snotel selected.")

    ## Process CSAS data (if selected)
    csas_t_df = pd.DataFrame()
    csas_a_df = pd.DataFrame()

    for site in csas_sel:
        csas_df = screen_csas(dtype, site, start_date, end_date)

        if site != "SBSG":
            csas_t_df[site] = csas_df["temp"]
        if (plot_albedo) and (site != "SBSG"):
            csas_a_df[site] = csas_df["albedo"]

    csas_max = np.nanmax([csas_t_df.max().max(),csas_a_df.max().max()])

    # Calculate plotting axes values
    ymin = np.nanmin([nws_t_min, snotel_t_min, 0])
    ymax = np.nanmax([nws_t_max, snotel_t_max, csas_max, freeze]) * 1.25
    ymax2 = np.nanmax([nws_p_max, snotel_p_max, 0.2]) * 5

    # Create figure
    print("Updating meteorology plot...")
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=dates,
        y=[freeze] * len(dates),
        showlegend=False,
        text="Degrees (F)",
        mode='lines',
        line=dict(color='grey', dash="dash"),
        name=str(freeze) + "F",
        yaxis="y1"
    ))

    fig.add_trace(go.Scatter(
        x=nws_t_df.index,
        y=nws_t_df["mean"],
        text="Degrees (F)",
        mode='lines',
        line=dict(color="black"),
        name="NWS Mean Temp for selection",
        yaxis="y1"
    ))

    fig.add_trace(go.Bar(
        x=nws_p_df.index,
        y=nws_p_df["mean"],
        text="Precip (in)",
        marker_color="black",
        showlegend=False,
        name="NWS Mean Precip for selection",
        yaxis="y2"
    ))

    for s in snotel_sel:
        fig.add_trace(go.Bar(
            x=snotel_p_df.index,
            y=snotel_p_df.loc[:, s],
            marker_color=snotel_sites.loc[s, "prcp_color"],
            text="Precip (in)",
            showlegend=False,
            name=name_df.loc[s, "name"] + " Daily Precip.",
            yaxis="y2"
        ))

        fig.add_trace(go.Scatter(
            x=snotel_t_df.index,
            y=snotel_t_df.loc[:, s],
            mode='lines',
            line=dict(color=snotel_sites.loc[s, "color"]),
            text="Degrees (F)",
            name=name_df.loc[s, "name"] + " Avg. Temp.",
            yaxis="y1"
        ))

    for c in csas_t_df.columns:
        fig.add_trace(go.Scatter(
            x=csas_t_df.index,
            y=csas_t_df[c],
            text="Degrees (F)",
            mode='lines',
            line=dict(color=csas_gages.loc[c, "color"],dash="dot"),
            name=c+" Avg. Temp.",
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
                yaxis="y1"))


    fig.add_trace(shade_forecast(ymax))

    fig.update_layout(
        margin={'l': 40, 'b': 40, 't': 0, 'r': 40},
        height=400,
        legend={'x': 0, 'y': 1, 'bgcolor': 'rgba(255,255,255,0.8)'},
        hovermode='closest',
        plot_bgcolor='white',
        xaxis=dict(
            range=[dates.min(), dates.max()],
            showline=True,
            linecolor="black",
            mirror=True
        ),
        yaxis=dict(
            title=ylabel,
            range=[ymin, ymax],
            showline=True,
            linecolor="black",
            mirror=True
        ),
        yaxis2=dict(
            title=ylabel2,
            side="right",
            overlaying='y',
            range=[ymax2, 0]
        )
    )
    return fig
