# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 08:40:21 2021

@author: buriona
"""

import os
import sys
from pathlib import Path
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
import dash

this_dir = os.path.dirname(os.path.realpath(__file__))

def create_app(db_path):
    app = dash.Dash(
        __name__,
    )
    snodas_all_db_path = Path(db_path, 'snodas.db')
    snodas_swe_db_path = Path(db_path, 'swe.db')
    snodas_sd_db_path = Path(db_path, 'sd.db')
    print(snodas_sd_db_path)
    snodas_all_db_con_str = f'sqlite:///{snodas_all_db_path.as_posix()}'
    snodas_swe_db_con_str = f'sqlite:///{snodas_swe_db_path.as_posix()}'
    snodas_sd_db_con_str = f'sqlite:///{snodas_sd_db_path.as_posix()}'
    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.server.config['SQLALCHEMY_DATABASE_URI'] = snodas_all_db_con_str
    app.server.config['SQLALCHEMY_BINDS'] = {
        'snodas_swe': snodas_swe_db_con_str,
        'snodas_sd': snodas_sd_db_con_str
    }

    return app

app = create_app(this_dir)
db = SQLAlchemy(app.server)
db.reflect()
print(f"{app.server.config['SQLALCHEMY_BINDS']}")
binds = db.get_binds()
SENSOR = 'swe' # or 'snowdepth'
slopes = [0, 80]
elrange = [4000, 13000]
s_date = '2000-03-01'
e_date = '2020-04-01'

bind_dict = {
    'swe': 'snodas_swe',
    'snowdepth': 'snodas_sd'
}
bind = bind_dict[SENSOR]
print(bind)
basins = db.get_tables_for_bind()
basin = basins[-1]
engine = db.get_engine(bind=bind)
qry = (
    f"select * from {basin}"#" where "
    # f"`Date` >= '{s_date}' "
    # f"and `Date` <= '{e_date}' "
    # f"and slope_d >= {slopes[0]} "
    # f"and slope_d <= {slopes[1]} "
    # f"and elev_ft >= {elrange[0]} "
    # f"and elev_ft <= {elrange[1]} "
)
print(f'Query: {qry}')

df = pd.read_sql(
    qry, 
    db.get_engine(bind=bind), 
    parse_dates=['Date'],
)
sys.exit(0)
