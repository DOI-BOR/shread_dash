"""
Created on Wed Jan 27, 2022

SHREAD Dash CSAS Plot

Script for running the CSAS plot in the dashboard (shread_dash.py)

@author: buriona, tclarkin (2020-2022)

"""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plot_lib.utils import screen_csas
from database import csas_gages, dust_ts, dust_layers
from hydroimport import import_csas_live

def get_csas_plot(start_date, end_date, plot_dust, csas_sel, dtype, plot_albedo,offline=True):
    """
    :description: this function updates the snowplot
    :param start_date: start date (from date selector)
    :param end_date: end date (from date selector)
    :param plot_dust: boolean for plotting dust layers
    :param csas_sel: list of selected csas sites ([])
    :param dtype: data type (dv/iv)
    :return: update figure
    """

    # Create date axis
    dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')
    # Set snow type based on user selection
    cvar = "Sno_Height_M"
    ylabel = ""

    csas_f_df = pd.DataFrame()
    csas_a_df = pd.DataFrame()
    csas_s_df = pd.DataFrame()

    for site in csas_sel:
        if offline:
            csas_df = screen_csas(site, start_date, end_date,dtype)
        else:
            csas_df = import_csas_live(site,start_date,end_date,dtype)

        #print(csas_df)

        if site == "SBSG":
            csas_f_df[site] = csas_df["flow"]
            if ylabel=="":
                ylabel = "Flow (ft^3/s)"
            else:
                ylabel = f"{ylabel} | Flow (ft^3/s)"
        elif site != "PTSP":
            csas_s_df[site] = csas_df["snwd"]
            if ylabel=="":
                ylabel = "Depth (in)"
            else:
                ylabel = f"{ylabel} | Depth (in)"
        if (plot_albedo) and (site != "SBSG"):
            csas_a_df[site] = csas_df["albedo"]

    csas_max = np.nanmax([csas_f_df.max().max(),csas_s_df.max().max()])

    ### Plot the data
    ymax = max([csas_max * 1.25, 20])

    print("Updating csas plot...")

    fig = go.Figure()

    for c in csas_sel:
        if c == "SBSG":
            fig.add_trace(go.Scatter(
                x=csas_f_df.index,
                y=csas_f_df[c],
                text=c + " Flow",
                mode='lines',
                line=dict(color="green", dash="dot"),
                name=c + " Flow",
                yaxis="y1"))
        elif c == "PTSP":
            continue
        else:
            fig.add_trace(go.Scatter(
                x=csas_s_df.index,
                y=csas_s_df[c],
                text="Snow Depth (in)",
                mode='lines',
                line=dict(color=csas_gages.loc[c, "color"], dash="solid"),
                name=c))

    if plot_dust == True:
        for d in dust_ts.columns:
            fig.add_trace(go.Scatter(
                x=dust_ts.index,
                y=dust_ts[d],
                text="Dust Layer Height (in)",
                mode='lines+markers',
                line=dict(color=dust_layers.loc[d, "color"], dash="dot"),
                name=d))

    if plot_albedo == True:
        for c in csas_a_df.columns:
            fig.add_trace(go.Scatter(
                x=csas_a_df.index,
                y=(1-csas_a_df[c])*100,
                text="100% - Albedo",
                mode='lines',
                line=dict(color=csas_gages.loc[c, "color"], dash="dash"),
                name=c + " 100% - Albedo",
                yaxis="y2"))

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
            title=ylabel,
            side="left",
            range=[0, ymax],
            showline=True,
            linecolor="black",
            mirror=True
        ))

    if plot_albedo == True:
        fig.update_layout(
            yaxis2=dict(
                title="100% - Albedo",
                side="right",
                overlaying='y',
                range=[0, 100]),
            margin={'l': 40, 'b': 40, 't': 0, 'r': 40},
        )
    print('csas plot is done')

    return fig