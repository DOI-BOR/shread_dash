# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 08:40:21 2021

@author: buriona,tclarkin
"""

import os
import sys
from pathlib import Path
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
import dash

this_dir = os.path.dirname(os.path.realpath(__file__))
this_dir = Path('C:/Programs/shread_dash/database/FLOW')

def create_app(db_path):
    app = dash.Dash(
        __name__,
    )
    rfc_iv_db_path = Path(db_path, 'rfc_iv.db')
    rfc_dv_db_path = Path(db_path, 'rfc_dv.db')
    rfc_iv_db_con_str = f'sqlite:///{rfc_iv_db_path.as_posix()}'
    rfc_dv_db_con_str = f'sqlite:///{rfc_dv_db_path.as_posix()}'
    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.server.config['SQLALCHEMY_BINDS'] = {
        'rfc_iv': rfc_iv_db_con_str,
        'rfc_dv': rfc_dv_db_con_str
    }

    return app

app = create_app(this_dir)
db = SQLAlchemy(app.server)
db.reflect()
print(f"{app.server.config['SQLALCHEMY_BINDS']}")
binds = db.get_binds()
SENSOR = 'rfc_iv' # or 'usgs_iv'
fcst_dt = '2021-12-16'

bind_dict = {
    'rfc_iv':'rfc_iv',
    'rfc_dv':'rfc_dv',
}
bind = bind_dict[SENSOR]
print(bind)
sites = db.get_tables_for_bind()
for site in sites:
    engine = db.get_engine(bind=bind)
    qry = (
        f"select * from {site} where "
        f"`fcst_dt` >= '{fcst_dt}'"
    )
    print(f'Query: {qry}')

    df = pd.read_sql(
        qry,
        db.get_engine(bind=bind),
        parse_dates=['date','fcst_dt'],
    )
    print(df.max())
    #sys.exit(0)
