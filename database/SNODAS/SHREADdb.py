# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 16:35:45 2020

@author: tclarkin

Script to compile SHREAD data into single, flat databases (.csv). Default is for overwrite=False & verbose=False.
overwrite=False means the script will check which files are contained in the database, and only append missing files.
verbose=False means the script will run silently (no prints).
"""
# Import Data
import pandas as pd
import os
import time

def push_to_sql(df):
# TODO: This needs to create a single db for the csv then run db_mk function on that
    return

# Opens files, appends type, source and data and appends to dataframe
def data_ingest(in_file,drop_list):
    in_file_split = in_file.split("_")

    data_in = pd.read_csv(in_file)
    data_in["Type"] = in_file_split[1]
    data_in["Source"] = in_file_split[0]
    data_in["Date"] = pd.to_datetime(in_file_split[2], format="%Y%m%d %H:%M:%S")
    data_in["File"] = in_file

    data_in = data_in.drop(drop_list,axis=1)

    out_df = data_in
    out_df = out_df.reset_index()

    return(out_df)

# Define working directory (data directory)
this_dir = os.path.dirname(os.path.realpath(__file__))
#this_dir = os.path.join("C://Users//tclarkin//Documents//GitHub//SHREAD_plot")
os.chdir("C://Programs//SHREAD//data//database")  # Location of SHREAD data files
database = os.path.join(this_dir, 'database')
database = this_dir

to_sql = False
overwrite = False
verbose = False
delete = False
drop_list = [
    "max", "min", "median", "Join_Count", "TARGET_FID", "pointid", "grid_code",
    "elev_m", "POLY_SOURC", "TOTAL_ID", "TOTAL_NAME"
]

# Define files in database
start = time.time()
data_files = os.listdir()
tif_files = list()
data_file_summary = pd.DataFrame()
for in_file in data_files:
    if verbose==True:
        print(in_file)

    if "tif" in in_file:
        tif_files.append(in_file)
        if verbose == True:
            print("Skipped")
        continue

    if "poly" in in_file:
        continue

    # Skip database (db) files
    if "db" in in_file:
        if verbose == True:
            print("Skipped")
        continue

    in_file_split = in_file.split("_")
    data_file_summary = data_file_summary.append([[in_file,in_file_split[0],in_file_split[1],in_file_split[2]]])
end = time.time()
if verbose == True:
    print("{} seconds to compile list of files.".format(round(end - start,1)))

### Import SHREAD Data ###
# Parse files (select csv files, open, append date, append to database)
sources = pd.unique(data_file_summary.loc[:,1])
if verbose == True:
    print("Overwrite set to {}".format(overwrite))
for source in sources:
    if verbose == True:
        print("Importing data from {}:".format(source))
    types = pd.unique(data_file_summary.loc[data_file_summary.loc[:,1]==source,2])
    for type in types:
        start = time.time()
        if verbose == True:
            print("{}...".format(type))
        all_files = pd.unique(data_file_summary.loc[(data_file_summary.loc[:,1]==source) & (data_file_summary.loc[:,2]==type),0])
        out_file = os.path.join(database,"{}_{}_db.csv".format(source,type))

        if overwrite == False:
            print("Comparing to db...")
            try:
                db = pd.read_csv(out_file,low_memory=False)
            except FileNotFoundError:
                db = pd.DataFrame(columns=["File"])
            in_files = [in_file for in_file in all_files if in_file not in db.File.unique()]
            end = time.time()
            print("{} seconds to compare {} from {} with db...".format(round(end - start, 1), type, source))
            print("Adding files to db...")
        else:
            db = pd.DataFrame()

        for in_file in in_files:
            if verbose == True:
                print(in_file)
            data = data_ingest(in_file,drop_list)
            db = db.append(data,ignore_index=True)

        db.to_csv(out_file,index=False)
        if to_sql:
            push_to_sql(db)
        end = time.time()
        print("{} seconds to compile {} from {}.".format(round(end - start,1),type,source))

        if delete==True:
            print("Deleting files from previous compiles...")
            del_files = [del_file for del_file in all_files if del_file not in in_files]

            for del_file in del_files:
                os.remove(del_file)
            for tif_file in tif_files:
                os.remove(tif_file)

print("Complete")

# delete
# import pandas as pd
# for sensor in ["swe","snowdepth"]:
#     db = pd.read_csv("database/snodas_{}_db.csv".format(sensor))
#     for col in db.columns:
#         if "Unnamed" in col:
#             db = db.drop(col,axis=1)
#         if col in drop_list:
#             db = db.drop(col,axis=1)
#
#     db.to_csv("database/snodas_{}_db_rev.csv".format(sensor),index=False)