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
this_dir = Path('C:/Programs/shread_plot/database/CSAS')

def create_app(db_path):
    app = dash.Dash(
        __name__,
    )
    #snodas_all_db_path = Path(db_path, 'snodas.db')
    snodas_csas1_db_path = Path(db_path, 'csas_iv.db')
    snodas_csas24_db_path = Path(db_path, 'csas_dv.db')
    #snodas_all_db_con_str = f'sqlite:///{snodas_all_db_path.as_posix()}'
    snodas_csas1_db_con_str = f'sqlite:///{snodas_csas1_db_path.as_posix()}'
    snodas_csas24_db_con_str = f'sqlite:///{snodas_csas24_db_path.as_posix()}'
    app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    #app.server.config['SQLALCHEMY_DATABASE_URI'] = snodas_all_db_con_str
    app.server.config['SQLALCHEMY_BINDS'] = {
        'csas_iv': snodas_csas1_db_con_str,
        'csas_dv': snodas_csas24_db_con_str
    }

    return app

app = create_app(this_dir)
db = SQLAlchemy(app.server)
db.reflect()
print(f"{app.server.config['SQLALCHEMY_BINDS']}")
binds = db.get_binds()
SENSOR = 'csas_dv' # or 'csas_iv'
s_date = '2020-03-01'
e_date = '2021-12-14'

bind_dict = {
    'csas_iv': 'csas_iv',
    'csas_dv': 'csas_dv'
}
bind = bind_dict[SENSOR]
print(bind)
basins = db.get_tables_for_bind()
for basin in basins:
    engine = db.get_engine(bind=bind)
    qry = (
        f"select * from {basin} where "
        f"`date` >= '{s_date}' "
        f"and `date` <= '{e_date}' "
    )
    print(f'Query: {qry}')

    df = pd.read_sql(
        qry,
        db.get_engine(bind=bind),
        parse_dates=['date'],
    )
    print(df.max())
    #sys.exit(0)
