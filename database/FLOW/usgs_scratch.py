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
this_dir = Path('C:/Programs/shread_plot/database/FLOW')

def create_app(db_path):
    app = dash.Dash(
        __name__,
    )
    usgs_iv_db_path = Path(db_path, 'usgs_iv.db')
    usgs_dv_db_path = Path(db_path, 'usgs_dv.db')
    usgs_iv_db_con_str = f'sqlite:///{usgs_iv_db_path.as_posix()}'
    usgs_dv_db_con_str = f'sqlite:///{usgs_dv_db_path.as_posix()}'
    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.server.config['SQLALCHEMY_BINDS'] = {
        'usgs_iv': usgs_iv_db_con_str,
        'usgs_dv': usgs_dv_db_con_str,
    }

    return app

app = create_app(this_dir)
db = SQLAlchemy(app.server)
db.reflect()
print(f"{app.server.config['SQLALCHEMY_BINDS']}")
binds = db.get_binds()
SENSOR = 'usgs_dv' # or 'usgs_iv'
s_date = '2021-12-05'
e_date = '2021-12-17'

bind_dict = {
    'usgs_iv':'usgs_iv',
    'usgs_dv':'usgs_dv',
}
bind = bind_dict[SENSOR]
print(bind)
sites = db.get_tables_for_bind()
for site in sites:
    engine = db.get_engine(bind=bind)
    qry = (
        f"select * from {site} where "
        f"`date` >= '{s_date}' "
        f"and `date` <= '{e_date}' "
    )
    print(f'Query: {qry}')

    df = pd.read_sql(
        qry,
        db.get_engine(bind=bind),
        parse_dates=['date','fcst_dt'],
    )
    print(df.max())
    #sys.exit(0)
