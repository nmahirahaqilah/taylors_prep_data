import pandas as pd
import warnings
import os
from r2r_pipelines import export_db

from config.constants import CPP_DATA_PATH, CPP_ENREG_PATH, CLEAN_DATA_PATH

warnings.filterwarnings("ignore")

# Functions to process historical CPP data (Prior to 2023)
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

def process_enreg_historical():
    print("Start Processing: Historical CPP Enreg Data")
    enreg_df = pd.read_excel(CPP_DATA_PATH + "/cpp_data_original.xlsx", sheet_name="enreg")

    # rename columns, convert to lower case and add underscore for spaces
    enreg_df.columns = enreg_df.columns.str.lower().str.replace(" ", "_")
    enreg_df.rename(columns={"cycle": "intake_cycle",
                             "team": "segment",
                             "type": "cpp_version",}, inplace=True)

    # Apply the filter rules
    enreg_df = filter_cpp_enreg(enreg_df)

    # Remove rows with null values in the enrollment column
    enreg_df = enreg_df[enreg_df["enrollment"].notnull()].reset_index(drop=True)
    
    enreg_df['cpp_version'] = enreg_df['cpp_version'].str.lower()
    
    # split the segment column into two columns: campus and segment, and get market_segment
    enreg_df['market_segment'] = enreg_df['segment'].str.split(' - ', expand=True)[1]

    # replace Progression with Domestic for standardization
    enreg_df['market_segment'] = enreg_df['market_segment'].replace({'Progression':'Domestic'}, regex=True)
    
    # Reorder columns
    enreg_df = enreg_df[['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'market_segment', 'cpp_version', 'ly_enrollment', 'ly_registration', 'enrollment', 'registration']]
    enreg_df.rename(columns = {"enrollment":"tgt_enrollment", "registration":"tgt_registration"}, inplace=True)
    
    return enreg_df

# Functions to process CPP files (2023 onwards)
def process_file_name(file_name):
    file_name = file_name.split("_")
    file_name = file_name[0].split(" ")

    intake_year = int(file_name[0])
    intake_cycle = file_name[1]
    cpp_version = file_name[2].lower()

    return intake_year, intake_cycle, cpp_version

def process_enreg_data(df, intake_year, intake_cycle, cpp_version, sheet):
    df = df.iloc[:, :4]
    df.columns = ['reporting_date', 'campus', 'segment', sheet]

    # Remove rows with missing values from the reporting_date column
    df = df.dropna(subset=['reporting_date'])
    df['intake_year'] = intake_year
    df['intake_cycle'] = intake_cycle
    df['cpp_version'] = cpp_version
    
    return df

def process_actual_and_target_data(file_path, intake_year, intake_cycle, cpp_version):
    # Process Actual Last Year Enreg Data
    enr_df = pd.read_excel(file_path, sheet_name="CTD E Actual " + str(intake_year - 1))
    enr_df = process_enreg_data(enr_df, intake_year, intake_cycle, cpp_version, "ly_enrollment")

    reg_df = pd.read_excel(file_path, sheet_name="CTD R Actual " + str(intake_year - 1))
    reg_df = process_enreg_data(reg_df, intake_year, intake_cycle, cpp_version, "ly_registration")

    ly_df = pd.merge(enr_df, reg_df, 
                    on=['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'cpp_version'], 
                    how='right')

    # Process CTD targets data
    enr_df = pd.read_excel(file_path, sheet_name="CTD E Targets " + str(intake_year))
    enr_df = process_enreg_data(enr_df, intake_year, intake_cycle, cpp_version, "tgt_enrollment")

    reg_df = pd.read_excel(file_path, sheet_name="CTD R Targets " + str(intake_year))
    reg_df = process_enreg_data(reg_df, intake_year, intake_cycle, cpp_version, "tgt_registration")

    tgt_df = pd.merge(enr_df, reg_df, 
                    on=['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'cpp_version'], 
                    how='right')
    
    # Joining the Actual last year and CTD Target data
    full_df = pd.merge(ly_df, tgt_df, 
                       on=['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'cpp_version'], 
                       how='right')
    
    # split the segment column into two columns: campus and segment, and get market_segment
    full_df['market_segment'] = full_df['segment'].str.split(' - ', expand=True)[0]

    # Replace ISR with International, and string start with Dom with Domestic
    full_df['market_segment'] = full_df['market_segment'].replace({'ISR': 'International',
                                                                    r'^Dom.*': 'Domestic'}, regex=True)
        
    # Reorganize the columns
    full_df = full_df[['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'market_segment', 'cpp_version', 'ly_enrollment', 'ly_registration', 'tgt_enrollment', 'tgt_registration']]
    
    return full_df

def process_enreg_cpp_files():
    # create a list of all files in the cpp enreg raw data folder
    files = os.listdir(CPP_ENREG_PATH)
    
    enreg_cpp = pd.DataFrame()
    
    # Loop through the list of cpp enreg files from the raw data folder
    for file_name in files:
        if file_name.endswith(".xlsx"):
            print(f'Start Processing: {file_name}')
            intake_year, intake_cycle, cpp_version = process_file_name(file_name)
            file_path =  CPP_ENREG_PATH + "/" + file_name
            
            full_df = process_actual_and_target_data(file_path, intake_year, intake_cycle, cpp_version)
            enreg_cpp = pd.concat([enreg_cpp, full_df])
            
    print('Completed Processing CPP EnReg Files')
    return enreg_cpp

def melt_and_merge_final_df(final_df):
    melted_df = pd.melt(
        final_df,
        id_vars=['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'market_segment', 'cpp_version'],
        value_vars=['tgt_enrollment', 'tgt_registration'],
        var_name='ctd_tgt_stage',
        value_name='ctd_tgt'
    )
    melted_df["ctd_tgt_stage"] = melted_df["ctd_tgt_stage"].replace({
        'tgt_enrollment': 'Enrollment',
        'tgt_registration': 'Registration'
    })

    ly_melt = pd.melt(
        final_df,
        id_vars=['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'market_segment', 'cpp_version'],
        value_vars=['ly_enrollment', 'ly_registration'],
        var_name='ctd_tgt_stage',
        value_name='ly_actual'
    )
    ly_melt["ctd_tgt_stage"] = ly_melt["ctd_tgt_stage"].replace({
        'ly_enrollment': 'Enrollment',
        'ly_registration': 'Registration'
    })

    merged_df = pd.merge(
        melted_df, ly_melt,
        on=['reporting_date', 'intake_year', 'intake_cycle', 'campus', 'segment', 'market_segment', 'cpp_version', 'ctd_tgt_stage'],
        how='inner'
    )
    return merged_df

# main() function
def preprocess_cpp_enreg_data():
    historical_enreg_df = process_enreg_historical()
    enreg_cpp = process_enreg_cpp_files()
    
    full_enreg_cpp = pd.concat([historical_enreg_df, enreg_cpp]).reset_index(drop=True)
    full_enreg_cpp = melt_and_merge_final_df(full_enreg_cpp)
    
    full_enreg_cpp.rename(columns={"reporting_date": "reporting_date", 
                             "intake_year": "intake_year", 
                             "intake_cycle": "intake_cycle", 
                             "campus": "campus", 
                             "segment": "segment",
                             "market_segment": "market_segment",
                             "cpp_version": "cpp_version", 
                             "ly_enrollment": "ly_enrollment", 
                             "ly_registration": "ly_registration", 
                             "tgt_enrollment": "ctd_tgt_enrollment", 
                             "tgt_registration": "ctd_tgt_registration"}, inplace=True)
    
    full_enreg_cpp.to_excel(CLEAN_DATA_PATH + "/cleaned_cpp_enreg.xlsx", index=False)

    engine = export_db.marcommdb_connection()
    full_enreg_cpp.to_sql('cpp_enreg', engine, schema='public', if_exists='replace', index=False)

    return full_enreg_cpp