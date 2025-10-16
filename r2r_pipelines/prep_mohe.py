import numpy as np
import pandas as pd
import warnings
import os
from config.constants import RM_MOHE_PATH, MAPPING_PATH, CLEAN_DATA_PATH

# ignore warnings
warnings.filterwarnings('ignore')

def read_and_clean_mohe_data():
    files = os.listdir(RM_MOHE_PATH)
    
    # There should be only one active excel file in the folder
    for file_name in files:
        if file_name.endswith('.xlsx'):
            mohe_df = pd.read_excel(RM_MOHE_PATH + '/' + file_name, sheet_name="TE")
    
    # read the full mohe dataset from redmarch
    #mohe_df = pd.read_excel(raw_data_path + "Redmarch - IPTS Enrolment Database 2023 v12 CLIENT (Raw data).xlsx", sheet_name="TE")
    
    # convert column names to lowercase and replace spaces with underscores in mohe_df
    mohe_df.columns = mohe_df.columns.str.lower().str.replace(" ", "_")

    # remove empty rows from IPTS column
    mohe_df = mohe_df.dropna(subset=["group"]).reset_index(drop=True)
    
    # replace "-" with np.nan in 'te' column
    mohe_df['te'] = mohe_df['te'].replace("-", np.nan)

    # rename level column to level_2
    mohe_df.rename(columns={'level_2': 'level'}, inplace=True)
    
    return mohe_df

def read_and_clean_prog_master():
    # read programme master file mapping
    prog_master = pd.read_excel(MAPPING_PATH + '/prog_master_file.xlsx', sheet_name="prog_master")

    # remove empty rows from level column from prog_master table
    prog_master = prog_master.dropna(subset=["level"]).reset_index(drop=True)
    
    return prog_master

def process_rules(prog_master):
    rules_df = prog_master[['prog_name_main', 'level', 'vertical', 'specialization', 'group', 'ipts']]
    
    # convert values in "specialization" column to list, split by ";"
    rules_df['specialization'] = rules_df['specialization'].str.split(';')

    # remove empty rows from specialization column
    rules_df = rules_df\
        .explode('specialization')\
        .dropna(subset=['specialization'])\
        .reset_index(drop=True)
    
    return rules_df

# Processing Programme Master file to identify possible labels for each row in MOHE data
prog_master = read_and_clean_prog_master()
rules_df = process_rules(prog_master)

# Create a list of all possible matches for each row
def get_matching_labels(row):
    matches = rules_df[
        (rules_df["level"] == row["level"]) &
        (rules_df["vertical"] == row["vertical"]) &
        (rules_df["specialization"] == row["specialization"])
    ]
    return matches["prog_name_main"].tolist()

# Resolve conflicts where multiple labels exist (use this for the filter in data mart)
def resolve_label(possible_labels):
    if len(possible_labels) == 1:
        return possible_labels[0]  # Single match
    elif len(possible_labels) > 1:
        return "|".join(possible_labels)  # Combine labels for conflicts
    else:
        return "Unlabeled"  # No match

# Process and save the MOHE data
def preprocess_mohe_data():
    mohe_df = read_and_clean_mohe_data()
    mohe_df['possible_labels'] = mohe_df.apply(get_matching_labels, axis=1)
    mohe_df['prog_label_count'] = mohe_df['possible_labels'].apply(len)
    mohe_df['prog_name_main'] = mohe_df['possible_labels'].apply(resolve_label)
    
    # drop 'possible_labels' column
    mohe_df.drop(columns=['possible_labels'], inplace=True)
    mohe_df['year'] = mohe_df['year'].astype(int)
    
    mohe_df.to_excel(CLEAN_DATA_PATH + "/cleaned_mohe_prog_labels.xlsx", index=False)


    return mohe_df