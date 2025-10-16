import pandas as pd
import warnings
import os
import numpy as np
from r2r_pipelines import export_db

from config.constants import CPP_DATA_PATH, CPP_NR_PATH, CLEAN_DATA_PATH

warnings.filterwarnings("ignore")

# Functions to process historical NR CPP data (prior to 2023)
# CPP version filter
def filter_cpp_enreg(df):
    filter_rules = [
    {"intake_year": 2021, "cpp_version": ["Budget"]},
    {"intake_year": 2022, "cpp_version": ["Budget", "Stretch"]}]
    
    filtered_rows = []

    for rule in filter_rules:
        filtered_rows.append(df[(df["intake_year"] == rule["intake_year"]) 
                                & (df["cpp_version"].isin(rule["cpp_version"]))])

    filtered_df = pd.concat(filtered_rows)
    return filtered_df

# Process historical NR data
def process_nr_historical(sheet):
    df = pd.read_excel(CPP_DATA_PATH + "/cpp_data_original.xlsx", sheet_name=sheet)

    # rename columns, convert to lower case and add underscore for spaces
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Remove irrelevant columns
    df.drop(columns=["ctd_student_number_target", 'cycle_end'], inplace=True)

    # Rename columns for consistency
    df.rename(columns={"cycle": "intake_cycle",
                       "type": "cpp_version"}, inplace=True)

    # Filter out CPP data for CPP versions that are not relevant for the analysis
    df = filter_cpp_enreg(df)

    # remove rows with no ctd_nr_target
    df = df[df["ctd_nr_target"].notnull()].reset_index(drop=True)
    
    df['cpp_version'] = df['cpp_version'].str.lower()
    df['stage_target'] = sheet.split("_")[1]
    
    return df

def consolidate_nr_historical():
    enr_df = process_nr_historical("nr_enrollment")
    reg_df = process_nr_historical("nr_registration")
    
    historical_nr_df = pd.concat([enr_df, reg_df], ignore_index=True)
    
    return historical_nr_df

# Functions to process CPP files (2023 onwards)
def process_file_name(file_name):
    file_name = file_name.split("_")
    file_name = file_name[0].split(" ")

    intake_year = int(file_name[0])
    intake_cycle = file_name[1]
    cpp_version = file_name[2].lower()

    return intake_year, intake_cycle, cpp_version

def process_nr_data(df, intake_year, intake_cycle, cpp_version, sheet):
    df = df.iloc[:, :8]
    df.columns = ['reporting_date', 'campus', 'ctd_nr_target', 'ctd_gr_target', 'ctd_s&d_target_scholarships', 
                  'ctd_s&d_target_bursaries', 'chdr_target', 'ctd_agent_comm_target']

    df['intake_year'] = intake_year
    df['intake_cycle'] = intake_cycle
    df['cpp_version'] = cpp_version
    df['stage_target'] = sheet
    
    return df

def consolidate_nr_data(file_path, intake_year, intake_cycle, cpp_version):
    # Process Enrollment dataset
    enr_sheet = f"{intake_year} {intake_cycle} CTD NR target by week_E"
    enr_df = pd.read_excel(file_path, sheet_name=enr_sheet)
    enr_df = process_nr_data(enr_df, intake_year, intake_cycle, cpp_version, "enrollment")

    # Process Registration dataset
    reg_sheet = f"{intake_year} {intake_cycle} CTD NR target by week_R"
    reg_df = pd.read_excel(file_path, sheet_name=reg_sheet)
    reg_df = process_nr_data(reg_df, intake_year, intake_cycle, cpp_version, "registration")

    df = pd.concat([enr_df, reg_df], ignore_index=True)
    
    return df

def process_nr_cpp_files():
    files = os.listdir(CPP_NR_PATH)

    nr_cpp = pd.DataFrame()

    for file_name in files:
        if file_name.endswith(".xlsx"):
            print(f'Start Processing {file_name}')
            intake_year, intake_cycle, cpp_version = process_file_name(file_name)
            file_path = CPP_NR_PATH + "/" + file_name
            
            df = consolidate_nr_data(file_path, intake_year, intake_cycle, cpp_version)
            nr_cpp = pd.concat([nr_cpp, df], ignore_index=True)
            
    print('Completed Processing CPP NR Files')
    return nr_cpp

# Process historical NR data
def preprocess_cpp_nr_data():
    historical_nr = consolidate_nr_historical()
    nr_cpp = process_nr_cpp_files()

    full_nr_cpp = pd.concat([historical_nr, nr_cpp], ignore_index=True)
    full_nr_cpp.rename(columns={'intake_year': 'intake_year', 
                                'intake_cycle': 'intake_cycle', 
                                'reporting_date': 'reporting_date', 
                                'campus': 'campus',
                                'ctd_nr_target': 'ctd_tgt_nr', 
                                'ctd_gr_target': 'ctd_tgt_gr',
                                'ctd_s&d_target_scholarships': 'ctd_tgt_snd_scholarships',
                                'ctd_s&d_target_bursaries': 'ctd_tgt_snd_bursaries',
                                'chdr_target': 'ctd_tgt_chdr',
                                'ctd_agent_comm_target': 'ctd_tgt_agent_comm',
                                'cpp_version': 'tgt_version', 
                                'stage_target': 'ctd_tgt_stage'}, inplace=True)
    
    full_nr_cpp.to_excel(CLEAN_DATA_PATH + "/cleaned_cpp_nr.xlsx", index=False)
