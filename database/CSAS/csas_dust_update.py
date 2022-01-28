# -*- coding: utf-8 -*-
"""
Created on 04 Jan 2022

@author: tclarkin
"""
import sys
import os
from pathlib import Path
import pandas as pd
import datetime as dt

# Load directories and defaults
this_dir = Path(__file__).absolute().resolve().parent
#this_dir = Path('C:/Programs/shread_plot/database/CSAS')
data_dir = Path(this_dir,"csas_dust")

# Open csas dust file
csas_dust_file = Path(this_dir,"csas_dust.csv")
csas_dust = pd.read_csv(csas_dust_file)

# Print last entry
print("Last data entered:")
print(csas_dust.tail(1))

# Get user input
check = input("Continue to add data (Y/N)? ")
dust_update = csas_dust.copy()

while check=="Y":
    # copy csas dust data
    dust_update = csas_dust.copy()

    # enter data
    row = len(dust_update)
    for c in dust_update.columns:
        if "Date" in c:
            date = input("Input snowpit date (YYYYMMDD): ")
            table_dat = dt.datetime.strptime(date, "%Y%m%d").strftime("%d-%m-%y")
        elif "D" in c:
            table_dat = input(f"Input height for dust layer {c}: ")
        elif "Surface" in c:
            table_dat = input(f"Input height for snow surface: ")

        table_dat = table_dat.strip()
        if table_dat=="":
            table_dat = "NaN"

        dust_update.loc[row,c] = table_dat

    check = input("Add additional data (Y/N)? ")

if len(dust_update) == len(csas_dust):
    print("No data will be written. Close script.")
else:
    print("Here are the changes:")
    diff = len(dust_update)-len(csas_dust)
    compare = dust_update.tail(diff)
    print(compare)
    export = input("Overwrite csas_dust.csv with updated data (Y/N)? ")

    if export=="Y":
        # save a copy
        if os.path.isdir(data_dir) is False:
            os.mkdir(data_dir)
        last = csas_dust.tail(1).Date.item()
        csas_dust.to_csv(Path(data_dir,f'csas_dust_{last}.csv'))

        # Overwrite
        dust_update.to_csv(csas_dust_file,index=False)
    else:
        print("No data will be written. Please rerun script.")
