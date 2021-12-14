"""
Created on Wed Dec 23 16:35:45 2020

@author: tclarkin
"""
### Import Dependencies & Define Functions
					
import os
import json
import datetime as dt

import dash
import pandas as pd

import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import database
from database import basin_list
from database import start_date, end_date, forc_disable, dust_disable
from database import snotel_list, usgs_list, csas_list
from database import sloperange, elevrange, aspectdict, elevdict, slopedict

from plot_lib.utils import get_plot_config
from plot_lib.snow_plot import get_snow_plot
from plot_lib.met_plot import get_met_plot
from plot_lib.flow_plot import get_flow_plot
from plot_lib.csas_plot import get_csas_plot,get_csas_plot2

app = database.app

app_dir = os.path.dirname(os.path.realpath(__file__))

# Load in presets
res_dir = os.path.join(app_dir, 'resources')
try:
    presets = pd.read_csv(os.path.join(res_dir, "presets.csv"))
except FileNotFoundError:
    presets = pd.DataFrame()
for col in ["snotels","usgss","csass","elevations","aspects","slopes"]:
    presets[col] = presets[col].apply(lambda x: json.loads(x))

presets.index = presets.id

preset_options = list()
input_options = list()
for p in presets.index:
    preset_options.append(dbc.DropdownMenuItem(presets.loc[p,"name"], id=p))
    input_options.append(Input(p,"n_clicks"))

def get_navbar():
    return dbc.Navbar(
        [
            dbc.Col(
                html.Img(
                    src=app.get_asset_url('BofR-vert-cmyk.png'),
                    className='img-fluid'
                ),
                width=1
            ),
            dbc.Col(
                html.Div(
                    [
                        html.H1(['WCAO Dashboard'])
                    ]
                ),
                width=5
            ),
            dbc.Col(html.Div(
                [
                    dbc.DropdownMenu(
                        label="Time Window Presets",
                        color="light",
                        children=
                        [
                            dbc.DropdownMenuItem("Now", id="set_now"),
                            dbc.DropdownMenuItem("2022", id="2022_window"),
                            dbc.DropdownMenuItem("2021", id="2021_window"),
                            #dbc.DropdownMenuItem("2019", id="2019_retro"),
                            #dbc.DropdownMenuItem("2017", id="2017_retro"),
                            #dbc.DropdownMenuItem("2012", id="2012_retro"),
                        ],
                    )
                ]
            ),
                width=2
            ),
            dbc.Col(html.Div(
                [
                    dbc.DropdownMenu(
                        label="Managed Basin Presets",
                        color="light",
                        children=preset_options,
                    )
                ]
            ),
                width=2
            ),
            dbc.Col(html.Div(
                [
                    dbc.DropdownMenu(
                        label="Report Error",
                        color="light",
                        children=
                        [
                            dbc.Alert(
                                [
                                    "Please report errors to ",
                                    html.A("tclarkin@usbr.gov",href="mailto:tclarkin@usbr.gov",className="alert-link"),
                                ],
                                color = "warning"
                            )
                        ]
                    )
                ]
            ),
                width=2
            )
        ],
        className='mb-4'
    )

def get_layout():
    return html.Div(
        className="mx-2",
        children=[
            dbc.Row(
                [
                    get_navbar()
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.FormGroup(
                                [
                                    html.H4('Select basin:'),
                                    #dbc.RadioItems(
                                    #    id='basin',
                                    #    options=basin_list,
                                    #    value=None),
                                    dcc.Dropdown(
                                        id='basin',
                                        options=basin_list,
                                        placeholder="Select basin",
                                        value=[],
                                        multi=False),
                                    dbc.Label('Set basin filters:'),
                                    dcc.RangeSlider(
                                        id='elevations',
                                        min=elevrange[0],
                                        max=elevrange[1],
                                        step=1,
                                        allowCross=False,
                                        marks=elevdict,
                                        value=[elevrange[0], elevrange[1]]
                                    ),
                                    dcc.RangeSlider(
                                        id='slopes',
                                        min=sloperange[0],
                                        max=sloperange[1],
                                        step=1,
                                        allowCross=False,
                                        marks=slopedict,
                                        value=[sloperange[0], sloperange[1]]
                                    ),
                                    dcc.RangeSlider(
                                        id='aspects',
                                        min=-90,
                                        max=360,
                                        step=45,
                                        allowCross=False,
                                        marks=aspectdict,
                                        value=[0, 360]
                                    ),
                                    html.Div(
                                        id='mean_elevation'
                                    )
                                    ]
                                ),
                            ]
                        ),
                    dbc.Col(dbc.FormGroup(
                        [
                            html.H4('Select time options:'),
                            html.Div(html.P()),
                            dcc.DatePickerRange(
                                id='date_selection',
                                start_date=start_date,
                                end_date=end_date,
                            ),
                            #dbc.Button(
                            #    "Step 1 day",
                            #    id="1step_button",
                            #    color="secondary"
                            #),
                            #dbc.Button(
                            #    "Step 1 week",
                            #    id="7step_button",
                            #color = "dark"
                            #),
                            html.Div(html.P()),
                            dbc.RadioItems(
                                id='dtype',
                                options=[{'label': "Daily", 'value': "dv"},
                                         {'label': "Instantaneous", 'value': "iv"}],
                                value='dv',
                                inline=True
                            ),
                            dbc.Checkbox(
                                id='plot_forecast',
                            ),
                            dbc.Label(
                                "Include Forecast Data",
                                style=dict(marginLeft=10),
                                html_for="plot_forecast",
                            )
                        ]
                    )),
                    dbc.Col(dbc.FormGroup(
                        [
                            html.H4('Select other options:'),
                            dbc.RadioItems(
                                id='stype',
                                options=[{'label': "SWE", 'value': "swe"},
                                         {'label': "Snow Depth", 'value': "snowdepth"}],
                                value='snowdepth',
                                inline=True
                            ),
                            html.Div(html.P()),
                            dbc.Checkbox(
                                id='plot_forc',
                                disabled=forc_disable,
                            ),
                            dbc.Label(
                                "Include Radiative Forcing",
                                style=dict(marginLeft=10),
                                html_for="plot_forc"
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_dust',
                                disabled=dust_disable,
                            ),
                            dbc.Label(
                                "Include CSAS Dust Layers",
                                style=dict(marginLeft=10),
                                html_for="plot_dust"
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_albedo_met',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo with Meteo",
                                style=dict(marginLeft=10),
                                html_for="plot_albedo_met",
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_albedo_flow',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo with Flow",
                                style=dict(marginLeft=10),
                                html_for="plot_albedo_flow",
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_albedo_csas',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo with CSAS",
                                style=dict(marginLeft=10),
                                html_for="plot_albedo_csas",
                            )
                        ]
                    ))
                ]
            ),
    
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dbc.Label('Select point observation sites:'),
                            dcc.Dropdown(
                                id='snotel_sel',
                                options=snotel_list,
                                placeholder="Select SNOTEL sites",
                                value=[],
                                multi=True),
                            html.Div(html.B()),
                            dcc.Dropdown(
                                id='usgs_sel',
                                options=usgs_list,
                                placeholder="Select USGS gages",
                                value=[],
                                multi=True),
                            html.Div(html.B()),
                            dcc.Dropdown(
                                id='csas_sel',
                                options=csas_list,
                                placeholder="Select CSAS study plots",
                                value=[],
                                multi=True
                            )
                        ]
                    ))
                ]
            ),
            ## TODO: Change to figure with subplots.
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dcc.Graph(
                                id='snow_plot',
                                config=get_plot_config("dashboard_snow.jpg"),
                            ),
                        ]
                    ))
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dcc.Graph(
                                id='met_plot',
                                config = get_plot_config("dashboard_met.jpg")
                            ),
                        ]
                    ))
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dcc.Graph(
                                id='flow_plot',
                                config = get_plot_config("dashboard_flow.jpg")
                            ),
                        ]
                    ))
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dcc.Graph(
                                id='csas_plot',
                                config=get_plot_config("dashboard_csas.jpg")
                            ),
                        ]
                    ))
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dcc.Graph(
                                id='csas_plot2',
                                config=get_plot_config("dashboard_csas.jpg")
                            ),
                        ]
                    ))
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dbc.FormGroup(
                        [
                            dbc.Button(
                                "Retry CSAS",
                                id="csas_replot",
                                color="light"
                                ),
                            ]
                        ),
                        width=2
                    ),
                    dbc.Col(dbc.FormGroup(
                        [
                            html.Div(
                                id='csas_message',
                            )
                        ]
                    ))
                ]
            )
            # In development
            # dbc.Row(
            #     [
            #         dbc.Col(dbc.FormGroup(
            #             [
            #                 dcc.RangeSlider(
            #                     id='sync_pan',
            #                     min=unixTimeMillis(start_date),
            #                     max=unixTimeMillis(end_date),
            #                     value=[unixTimeMillis(start_date),unixTimeMillis(end_date)],
            #                     step=86400000,
            #                     marks=getMarks(start_date,end_date)
            #                     )
            #             ]
            #         ))
            #     ]
            # )
        ]
    )

app.layout = get_layout()

### INTERACTIONS

## In development
# @app.callback(
#     Output('sync_pan', 'min'),
#     Output('sync_pan', 'max'),
#     Output('sync_pan', 'value'),
#     Output('sync_pan', 'marks'),
#     [Input('date_selection', 'start_date'),
#      Input('date_selection', 'end_date'),
#      ]
# )
# def change_date_slider(start_date,end_date):
#     """
#     :description: this function changes the range of the date slider
#     :param start_date: the start date selected
#     :param end_date: the end date selected
#     :return: updated min, max, values and marks for slider
#     """
#
#     dates = pd.date_range(start_date,end_date,freq="D",tz="UTC")
#     start = dt.datetime.strptime(start_date, "%Y-%m-%d")
#     end = dt.datetime.strptime(end_date, "%Y-%m-%d")
#     minv = unixTimeMillis(start)
#     maxv = unixTimeMillis(end)
#     value = [unixTimeMillis(start),unixTimeMillis(end)]
#     if len(dates) < 30:
#         n = 24
#     elif (len(dates) >= 30) & (len(dates) <= 90):
#         n = 7*24
#     elif len(dates) > 90:
#         n = 30*24
#     marks = getMarks(start,end,Nth=n)
#
#     return(minv,maxv,value,marks)
#
# @app.callback(
#     Output('snow_plot', 'figure'),
#     Output('met_pan', 'figure'),
#     Output('flow_pan', 'figure'),
#     [Input('sync_pan', 'value'),
#     State('snow_plot', 'figure'),
#     State('met_plot', 'figure'),
#     State('flow_plot', 'figure')
#      ]
# )
# def sync_pan_figures(value,snow_fig,met_fig,flow_fig):
#     """
#     :description: this function changes the range of the dates in the figures
#     :param value: from range slider
#     :param figs: each of the figures being updated
#     :return: updated figure layouts
#     """
#     snow_fig["layout"] = {'xaxis':{'range': [unixToDatetime(value[0]),
#                                              unixToDatetime(value[1])]}}
#     met_fig["layout"] = {'xaxis' : {'range': [unixToDatetime(value[0]),
#                                                unixToDatetime(value[1])]}}
#     flow_fig["layout"] = {'xaxis' : {'range': [unixToDatetime(value[0]),
#                                                unixToDatetime(value[1])]}}
#     return(snow_fig,met_fig,flow_fig)

# @app.callback(
#     Output('csas_sel', 'disabled'),
#     Output('plot_albedo_flow', 'disabled'),
#     Output('plot_albedo_met', 'disabled'),
#     Output('plot_dust', 'disabled'),
#     Output('plot_albedo_csas','disabled'),
#     [Input('date_selection', 'start_date'),
#      Input('date_selection', 'end_date')]
# )
# def disable_csas(start_date,end_date):
#     """
#     :description: this function disables CSAS data for the current year...the csas website is too inconsistent to include.
#     :param start_date: the start date selected
#     :param end_date: the end date selected
#     :return: series of booleans (True/False)
#     """
#     if (start_date>"2020-12-30") & (end_date>"2020-12-30"):
#         print("csas disabled.")
#         return(False,True,True,False,False)
#     if (start_date<="2020-12-30") & (end_date<="2020-12-30"):
#         return(False,False,False,False,False)

@app.callback(
    Output('plot_forecast', 'disabled'),
    Output('plot_forecast', 'checked'),
    [Input('date_selection', 'end_date'),
     State('plot_forecast', 'checked')]
)
def disable_forecast(end_date,plot_forecast):
    """
    :description: this function disables forecast data if time window doesn't exctend to future.
    :param end_date: the end date selected
    :return: series of booleans (True/False)
    """
    end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
    today = dt.datetime.now()
    print(today)
    if end_date<today:
        print("forecasts disabled.")
        return(True,False)
    else:
        return(False,plot_forecast)

@app.callback(
    Output('basin', 'value'),
    Output('dtype', 'value'),
    Output('plot_forecast', 'value'),
    Output('plot_albedo_flow', 'value'),
    Output('stype', 'value'),
    Output('plot_forc', 'value'),
    Output('plot_dust', 'value'),
    Output('plot_albedo_met', 'value'),
    Output('snotel_sel', 'value'),
    Output('usgs_sel', 'value'),
    Output('csas_sel', 'value'),
    Output('elevations', 'value'),
    Output('aspects', 'value'),
    Output('slopes', 'value'),
    input_options
)
def load_presets(a,b,c,d,e):
    """
    :description: this function applies user specified presets based on dropdown menu clicks
    :return: user specified presets, as defined below.
    """
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    id = changed_id.split(".")[0]
    print(changed_id)
    print(id)
    if id not in presets.index:
        id = presets.index[0]
    # basins
    basins = presets.loc[id,"basins"]
    dtypes = presets.loc[id,"dtypes"]
    frcst = presets.loc[id,"frcst"]
    albedo_flow = presets.loc[id,"albedo_flow"]
    stypes = presets.loc[id,"stypes"]
    forcs = presets.loc[id,"forcs"]
    dusts = presets.loc[id,"dusts"]
    albedo_met = presets.loc[id,"albedo_met"]
    snotels = presets.loc[id,"snotels"]
    usgss = presets.loc[id,"usgss"]
    csass = presets.loc[id,"csass"]
    elevations = presets.loc[id,"elevations"]
    aspects = presets.loc[id,"aspects"]
    slopes = presets.loc[id,"slopes"]

    return(basins,dtypes,frcst,albedo_flow,stypes,forcs,dusts,albedo_met,snotels,usgss,csass,elevations,aspects,slopes)

@app.callback(
    Output('date_selection', 'start_date'),
    Output('date_selection', 'end_date'),
    [#Input('2012_retro', 'n_clicks'),
     #Input('2017_retro', 'n_clicks'),
     #Input('2019_retro', 'n_clicks'),
     Input('set_now', 'n_clicks'),
     Input('2021_window', 'n_clicks'),
     Input('2022_window', 'n_clicks'),
     #Input('7step_button','n_clicks'),
     #Input('1step_button','n_clicks'),
     State('date_selection', 'start_date'),
     State('date_selection', 'end_date')]
)
def load_preset_dates(a,b,c,start,end):
    """
    :description: this function applies user specified dates based on dropdown menu clicks
    :return: user specified dates
    """
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    # if '2012_retro' in changed_id:
    #     start_date = "2012-02-01"
    #     end_date = "2012-02-15"
    # elif '2017_retro' in changed_id:
    #     start_date = "2017-03-01"
    #     end_date = "2017-03-15"
    # elif '2019_retro' in changed_id:
    #     start_date = "2019-03-01"
    #     end_date = "2019-03-15"
    if '2021_window' in changed_id:
        start_date = "2021-01-01"
        end_date = "2021-07-01"
    elif '2022_window' in changed_id:
        start_date = "2021-11-01"
        end_date = dt.datetime.now().date() + dt.timedelta(days=10)
    # elif '7step_button' in changed_id:
    #     start_date = dt.datetime.strptime(start, "%Y-%m-%d") + dt.timedelta(days=7)
    #     start_date = dt.datetime.strftime(start_date, "%Y-%m-%d")
    #     end_date = dt.datetime.strptime(end, "%Y-%m-%d") + dt.timedelta(days=7)
    #     end_date = dt.datetime.strftime(end_date, "%Y-%m-%d")
    # elif '1step_button' in changed_id:
    #     start_date = dt.datetime.strptime(start, "%Y-%m-%d") + dt.timedelta(days=1)
    #     start_date = dt.datetime.strftime(start_date, "%Y-%m-%d")
    #     end_date = dt.datetime.strptime(end, "%Y-%m-%d") + dt.timedelta(days=1)
    #     end_date = dt.datetime.strftime(end_date, "%Y-%m-%d")
    else:
        start_date = dt.datetime.now().date() - dt.timedelta(days=10)
        start_date = dt.datetime.strftime(start_date, "%Y-%m-%d")
        end_date = dt.datetime.now().date() + dt.timedelta(days=10)
        end_date = dt.datetime.strftime(end_date, "%Y-%m-%d")
    return(start_date,end_date)

@app.callback(
    Output('snow_plot', 'figure'),
    Output('mean_elevation', 'children'),
    [
        Input('basin', 'value'),
        Input('stype', 'value'),
        Input('elevations', 'value'),
        Input('aspects', 'value'),
        Input('slopes', 'value'),
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('snotel_sel', 'value'),
    ])
def update_snow_plot(basin, stype, elrange, aspects, slopes, start_date, 
                     end_date, snotel_sel):
   
    fig, basin_stats = get_snow_plot(
        basin, stype, elrange, aspects, slopes, start_date, 
        end_date, snotel_sel
    )
    return fig, basin_stats

@app.callback(
    Output('met_plot', 'figure'),
    [
        Input('basin', 'value'),
        Input('plot_forc', 'checked'),
        Input('elevations', 'value'),
        Input('aspects', 'value'),
        Input('slopes', 'value'),
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('snotel_sel', 'value'),
        Input('csas_sel','value'),
        Input('plot_albedo_met','checked'),
        Input('dtype', 'value')
    ])
def update_met_plot(basin, plot_forc, elrange, aspects, slopes, start_date, 
                    end_date, snotel_sel, csas_sel, plot_albedo, dtype):
    
    fig = get_met_plot(
        basin, plot_forc, elrange, aspects, slopes, start_date, 
        end_date, snotel_sel, csas_sel, plot_albedo, dtype
    )
    return fig

@app.callback(
    Output('flow_plot', 'figure'),
    [
        Input('usgs_sel', 'value'),
        Input('dtype', 'value'),
        Input('plot_forecast', 'checked'),
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('csas_sel','value'),
        Input('plot_albedo_flow','checked')
    ])
def update_flow_plot(usgs_sel, dtype, plot_forecast, start_date, end_date,
                     csas_sel, plot_albedo):

    fig = get_flow_plot(
        usgs_sel, dtype, plot_forecast, start_date, end_date, csas_sel, plot_albedo
    )
    return fig

@app.callback(
    Output('csas_plot', 'figure'),
    Output('csas_message',"children"),
    Output('csas_replot','color'),
    [
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('plot_dust',"checked"),
        Input('csas_sel', 'value'),
        Input('dtype', 'value'),
        Input('plot_albedo_csas','checked'),
        Input('csas_replot','n_clicks'),
    ])
def update_csas_plot(start_date, end_date, plot_dust, csas_sel, dtype, albedo,n_clicks):

    fig,message,csas_color = get_csas_plot(start_date, end_date, plot_dust, csas_sel, dtype, albedo)

    return fig,message,csas_color

@app.callback(
    Output('csas_plot2', 'figure'),
    [
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('plot_dust',"checked"),
        Input('csas_sel', 'value'),
        Input('dtype', 'value'),
        Input('plot_albedo_csas','checked'),
        Input('csas_replot','n_clicks'),
    ])
def update_csas_plot2(start_date, end_date, plot_dust, csas_sel, dtype, albedo,n_clicks):

    fig = get_csas_plot2(start_date, end_date, plot_dust, csas_sel, dtype, albedo)

    return fig

### LAUNCH
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.server.run(debug=False)

    @app.after_request
    def after_request(response):
        for query in get_debug_queries():
            if query.duration >= 0:
                print(query.statement, query.parameters, query.duration, query.context)
        return response