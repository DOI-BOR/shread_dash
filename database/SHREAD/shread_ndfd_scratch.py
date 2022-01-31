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

#this_dir = os.path.dirname(os.path.realpath(__file__))
this_dir = Path('C:/Programs/shread_dash/database/SHREAD')

def create_app(db_path):
    app = dash.Dash(
        __name__,
    )
    mint_db_con_str = f'sqlite:///{Path(db_path, "mint.db").as_posix()}'
    maxt_db_con_str = f'sqlite:///{Path(db_path, "maxt.db").as_posix()}'
    rhm_db_con_str = f'sqlite:///{Path(db_path, "rhm.db").as_posix()}'
    pop12_db_con_str = f'sqlite:///{Path(db_path, "pop12.db").as_posix()}'
    qpf_db_con_str = f'sqlite:///{Path(db_path, "qpf.db").as_posix()}'
    snow_db_con_str = f'sqlite:///{Path(db_path, "snow.db").as_posix()}'
    sky_db_con_str = f'sqlite:///{Path(db_path, "sky.db").as_posix()}'

    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.server.config['SQLALCHEMY_BINDS'] = {
        'mint': mint_db_con_str,
        'maxt': maxt_db_con_str,
        'rhm': rhm_db_con_str,
        'pop12': pop12_db_con_str,
        'qpf': qpf_db_con_str,
        'snow': snow_db_con_str,
        'sky': sky_db_con_str,
    }

    return app

app = create_app(this_dir)
db = SQLAlchemy(app.server)
db.reflect()
print(f"{app.server.config['SQLALCHEMY_BINDS']}")
binds = db.get_binds()
SENSOR = 'mint' # or 'snowdepth'
slopes = [0, 80]
elrange = [4000, 13000]
s_date = '2022-01-28'
e_date = '2022-01-31'

bind_dict = {
    'mint': 'mint',
    'maxt': 'maxt',
    'pop12':'pop12',
    'qpf':'qpf',
    'snow':'snow'
}
bind = bind_dict[SENSOR]
print(bind)
basins = db.get_tables_for_bind()
basin = basins[0]
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
    parse_dates=['Date_Valid'],
)
print(df["Date_Valid"].min())
print(df["Date_Valid"].max())
#sys.exit(0)
