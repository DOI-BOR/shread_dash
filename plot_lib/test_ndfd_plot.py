# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from hydroimport import import_snotel,import_csas_live

from database import snotel_sites
from database import csas_gages
from plot_lib.utils import screen_spatial,ba_stats,screen_csas,screen_snotel
from plot_lib.utils import ba_min_plot, ba_max_plot, ba_mean_plot, ba_median_plot
from plot_lib.utils import shade_forecast

def get_test_plot(ndfd_sel,basin,elrange,aspects,slopes,start_date,end_date):
    """
    :description: this function updates the snowplot
    :param ndvd_sel: the selected "sensors" (checklist)
    :param basin: the selected basins (checklist)
    :param elrange: the range of elevations ([min,max])
    :param aspects: the range of aspects  ([min,max])
    :param slopes: the range of slopes ([min,max])
    :param start_date: start date (from date selector)
    :param end_date: end date (from date selector)
    :return: update figure
    """

    # Create date axis
    dates = pd.date_range(start_date, end_date, freq="D", tz='UTC')

    ## Process SHREAD data
    fig = go.Figure()

    # Add pop12:

    # Filter data
    if basin == None:
        print("No basins selected.")
        ndfd_plot = False
    elif len(ndfd_sel)>0:
        ndfd_plot = True
        if "snow" in str(ndfd_sel):
            ndfd_sel.append("pop12")
        mint = maxt = snow = pop12 = False
        for sensor in ndfd_sel:
            df = screen_spatial(sensor,start_date,end_date,basin,aspects,elrange,slopes,"Date")
            if df.empty:
                continue
            else:
                # Calculate basin average values
                ba_ndfd = ba_stats(df, "Date")

                if sensor=="mint":
                    mint = ba_ndfd
                if sensor=="maxt":
                    maxt = ba_ndfd
                if sensor=="snow":
                    snow = ba_ndfd
                if sensor=="pop12":
                    pop12 = ba_ndfd
    else:
        ndfd_plot = False

    fig.add_trace(shade_forecast(30))

    if ndfd_plot:
        if mint is not False:
            fig.add_trace(ba_mean_plot(mint, f"Min Temp","blue"))

        if maxt is not False:
            fig.add_trace(ba_mean_plot(maxt, f"Max Temp","red"))

        if snow is not False:

            fig.add_trace(go.Bar(
                x=snow.index,
                y=snow["mean"],
                text=pop12["mean"],
                marker_color="purple",
                showlegend=True,
                name="NWS Mean Snow Forecast for selection",
            ))

    fig.update_layout(
        xaxis=dict(
            range=[start_date, end_date],
            showline=True,
            linecolor="black",
            mirror=True
        ),
        yaxis=dict(
            title = "NDFD [-]",
            type = 'linear',
            #range = [0, ymax],
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

    print('test plot is done')
    
    return fig
