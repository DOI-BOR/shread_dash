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
from database import start_date, end_date, dust_disable
from database import snotel_list,usgs_list,csas_list,ndfd_list
from database import sloperange, elevrange, aspectdict, elevdict, slopedict

from plot_lib.utils import get_plot_config
from plot_lib.snow_plot import get_snow_plot
from plot_lib.met_plot import get_met_plot
from plot_lib.flow_plot import get_flow_plot
from plot_lib.csas_plot import get_csas_plot
from plot_lib.test_ndfd_plot import get_test_plot

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
                            ),
                            dbc.Label('Select NDFD variables:'),
                            dcc.Dropdown(
                                id='ndfd_sel',
                                options=ndfd_list,
                                placeholder="Select NDFD variables",
                                value=[],
                                multi=True),
                        ]
                    )),
                    dbc.Col(dbc.FormGroup(
                        [
                            html.H4('Select other options:'),
                            dbc.Checkbox(
                                id='offline',
                                checked=True,
                            ),
                            dbc.Label(
                                "Offline Mode",
                                style=dict(marginLeft=10),
                                html_for="offline",
                            ),
                            dbc.RadioItems(
                                id='stype',
                                options=[{'label': "SWE", 'value': "swe"},
                                         {'label': "Snow Depth", 'value': "sd"}],
                                value="sd",
                                inline=True
                            ),
                            html.Div(html.P()),
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
                                id='plot_albedo_snow',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo on Snow Plot",
                                style=dict(marginLeft=10),
                                html_for="plot_albedo_snow",
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_albedo_met',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo on Meteo Plot",
                                style=dict(marginLeft=10),
                                html_for="plot_albedo_met",
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_albedo_flow',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo on Flow Plot",
                                style=dict(marginLeft=10),
                                html_for="plot_albedo_flow",
                            ),
                            html.Div(html.B()),
                            dbc.Checkbox(
                                id='plot_albedo_csas',
                            ),
                            dbc.Label(
                                "Plot CSAS Albedo on CSAS Plot",
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
                                placeholder="Select FLOW gages",
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
                                id='test_plot',
                                config=get_plot_config("dashboard_test.jpg")
                            ),
                        ]
                    ))
                ]
            ),
        ]
    )

app.layout = get_layout()

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
    dusts = presets.loc[id,"dusts"]
    albedo_met = presets.loc[id,"albedo_met"]
    snotels = presets.loc[id,"snotels"]
    usgss = presets.loc[id,"usgss"]
    csass = presets.loc[id,"csass"]
    elevations = presets.loc[id,"elevations"]
    aspects = presets.loc[id,"aspects"]
    slopes = presets.loc[id,"slopes"]

    return(basins,dtypes,frcst,albedo_flow,stypes,dusts,albedo_met,snotels,usgss,csass,elevations,aspects,slopes)

@app.callback(
    Output('date_selection', 'start_date'),
    Output('date_selection', 'end_date'),
    [Input('set_now', 'n_clicks'),
     Input('2021_window', 'n_clicks'),
     Input('2022_window', 'n_clicks'),
     State('date_selection', 'start_date'),
     State('date_selection', 'end_date')]
)
def load_preset_dates(a,b,c,start,end):
    """
    :description: this function applies user specified dates based on dropdown menu clicks
    :return: user specified dates
    """
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]

    if '2021_window' in changed_id:
        start_date = "2021-01-01"
        end_date = "2021-07-01"
    elif '2022_window' in changed_id:
        start_date = "2021-11-01"
        end_date = dt.datetime.now().date() + dt.timedelta(days=10)
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
        Input('csas_sel','value'),
        Input('plot_albedo_snow','checked'),
        Input('offline','checked')
    ])
def update_snow_plot(basin, stype, elrange, aspects, slopes, start_date,
                     end_date, snotel_sel,csas_sel,plot_albedo,offline):

    fig, basin_stats = get_snow_plot(
        basin, stype, elrange, aspects, slopes, start_date,
        end_date, snotel_sel,csas_sel,plot_albedo,
        offline
    )
    return fig, basin_stats

@app.callback(
    Output('met_plot', 'figure'),
    [
        Input('basin', 'value'),
        Input('elevations', 'value'),
        Input('aspects', 'value'),
        Input('slopes', 'value'),
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('snotel_sel', 'value'),
        Input('csas_sel','value'),
        Input('plot_albedo_met','checked'),
        Input('dtype', 'value'),
        Input('ndfd_sel','value'),
        Input('offline','checked'),
    ])
def update_met_plot(basin, elrange, aspects, slopes, start_date,
                    end_date, snotel_sel, csas_sel, plot_albedo, dtype,
                    ndfd_sel,offline):

    fig = get_met_plot(
        basin, elrange, aspects, slopes, start_date,
        end_date, snotel_sel, csas_sel, plot_albedo, dtype,
        ndfd_sel,offline
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
        Input('plot_albedo_flow','checked'),
        Input('offline','checked'),
    ])
def update_flow_plot(usgs_sel, dtype, plot_forecast, start_date, end_date,
                     csas_sel, plot_albedo,
                     offline):

    fig = get_flow_plot(
        usgs_sel, dtype, plot_forecast, start_date, end_date, csas_sel, plot_albedo,
        offline
    )
    return fig

@app.callback(
    Output('csas_plot', 'figure'),
    [
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
        Input('plot_dust',"checked"),
        Input('csas_sel', 'value'),
        Input('dtype', 'value'),
        Input('plot_albedo_csas','checked'),
        Input('offline','checked'),
    ])
def update_csas_plot(start_date, end_date, plot_dust, csas_sel, dtype, albedo,offline):

    fig = get_csas_plot(start_date, end_date, plot_dust, csas_sel, dtype, albedo,offline)

    return fig

@app.callback(
    Output('test_plot', 'figure'),
    [
        Input('ndfd_sel','value'),
        Input('basin', 'value'),
        Input('elevations', 'value'),
        Input('aspects', 'value'),
        Input('slopes', 'value'),
        Input('date_selection', 'start_date'),
        Input('date_selection', 'end_date'),
    ])
def update_test_plot(ndfd_sel,basin,elrange,aspects,slopes,start_date,end_date):

    fig = get_test_plot(
        ndfd_sel,basin,elrange,aspects,slopes,start_date,end_date
    )
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

