import numpy as np
import pandas as pd
import warnings
from pathlib import Path
from config.constants import RM_MOHE_PATH, MAPPING_PATH

# ignore warnings
warnings.filterwarnings('ignore')


def extract_mohe_enrollment(file_path = RM_MOHE_PATH, file_name ="Redmarch - IPTS Enrolment Database 2023 v13 CLIENT (RAW DATA).xlsx"):
    # read the full mohe dataset from redmarch
    mohe_df = pd.read_excel(Path(file_path)/file_name, sheet_name="TE")
    
    # convert column names to lowercase and replace spaces with underscores in mohe_df
    mohe_df.columns = (mohe_df.columns.str
                       .lower()
                       .str.replace("-", "")
                       .str.replace(r"\s+", "_", regex=True)
    )

    # remove empty rows from IPTS column
    mohe_df = mohe_df.dropna(subset=["group"]).reset_index(drop=True)
    
    # replace "-" with np.nan in 'te' column
    mohe_df['te'] = mohe_df['te'].replace("-", np.nan)

    # rename level column to level_2
    mohe_df.rename(columns={'level_2': 'level'}, inplace=True)
    
    # convert values in "specialization", "level", and "vertical" columns to uppercase and remove whitespace
    for col in ['specialization', 'level', 'vertical', 'group']:
        mohe_df[col] = mohe_df[col].str.upper().str.strip()
    
    return mohe_df

def extract_prog_requirements(file_path = MAPPING_PATH, file_name = "prog_master_file.xlsx"):
    # read programme master file mapping
    prog_master = pd.read_excel(Path(file_path)/file_name, sheet_name="prog_master")

    # remove empty rows from level column from prog_master table
    prog_master = prog_master.dropna(subset=["level"]).reset_index(drop=True)

    rules_df = prog_master[['prog_name_main', 'level', 'vertical', 'specialization', 'group', 'ipts']].copy()
    
    # convert values in "specialization" column to list, split by ";"
    rules_df['specialization'] = rules_df['specialization'].str.split(';')

    # remove empty rows from specialization column
    rules_df = rules_df\
        .explode('specialization')\
        .dropna(subset=['specialization'])\
        .reset_index(drop=True)
    
    # convert values in "specialization", "level", and "vertical" columns to uppercase and remove whitespace
    for col in ['specialization', 'level', 'vertical', 'group']:
        rules_df[col] = rules_df[col].str.upper().str.strip()
    
    return rules_df

def assign_prog_labels(mohe_df, rules_df):
    # Create a list of all possible matches for each row. Can be refined to include rules for programme names
    def get_matching_labels(row):
        matches = rules_df[
            (rules_df["level"] == row["level"]) &
            (rules_df["vertical"] == row["vertical"]) &
            (rules_df["specialization"] == row["specialization"])
        ]
        return matches["prog_name_main"].tolist()

    # Resolve conflicts where multiple labels exist (use this for the filter in the BI Tool)
    def resolve_label(possible_labels):
        if len(possible_labels) == 1:
            return possible_labels[0]  # Single match
        elif len(possible_labels) > 1:
            return "|".join(possible_labels)  # Combine labels for conflicts
        else:
            return "Unlabeled"  # No match

    # Process and save the MOHE data
    mohe_df['possible_labels'] = mohe_df.apply(get_matching_labels, axis=1)
    mohe_df['prog_label_count'] = mohe_df['possible_labels'].apply(len)
    mohe_df['prog_name_main'] = mohe_df['possible_labels'].apply(resolve_label)

    # drop 'possible_labels' column
    mohe_df.drop(columns=['possible_labels'], inplace=True)
    mohe_df['year'] = mohe_df['year'].astype(int)
    
    return mohe_df

def preprocess_mohe_enrollment():
    mohe_df = (
        extract_mohe_enrollment()
        .pipe(lambda df: assign_prog_labels(df, extract_prog_requirements()))
    )
    
    return mohe_df