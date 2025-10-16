import pandas as pd
import numpy as np
import os
from r2r_pipelines.utils import extract_ict_calendar

from config.constants import CYCLE_CLOSING_PATH, MAPPING_PATH

def get_closing_file_info(file_name):
    # split the file name by "_" and "."
    parts = file_name.split(".")[0].split("_")
    
    intake_year = int(parts[1])
    intake_cycle = parts[2].upper()
    
    return intake_year, intake_cycle

def preprocess_closing_data(file_path = CYCLE_CLOSING_PATH):   
    cls_df = pd.DataFrame()
    relevant_cols = ['AccountID', 'OpportunityID', 'OpportunityName']
    
    # create a list of all files in the test folder
    files = os.listdir(CYCLE_CLOSING_PATH)
    
    # loop through all files in the cycle_closing folder
    for file_name in files:
        if file_name.endswith('.xlsx'):
            print('Processing file:', file_name)
            cls = pd.read_excel(os.path.join(file_path, file_name))
            cls = cls[relevant_cols].copy()
            cls.rename(columns={'AccountID': 'acc_id', 'OpportunityID': 'opp_id', 'OpportunityName': 'opp_name'}, inplace=True)
            cls['intake_year'], cls['intake_cycle'] = get_closing_file_info(file_name)
            cls_df = pd.concat([cls_df, cls], ignore_index=True)
    
    # Extract and merge with academic calendar to get cycle start and end dates
    acad_calendar = extract_ict_calendar()
    cls_df = cls_df.merge(acad_calendar[['prog_intake_year', 'cycle', 'cycle_start_date', 'cycle_end_date']],
                      left_on=['intake_year', 'intake_cycle'], right_on=['prog_intake_year', 'cycle'], how='left')
    cls_df.drop(columns=['prog_intake_year', 'cycle'], inplace=True)
    
    return cls_df