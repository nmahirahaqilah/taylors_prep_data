import pandas as pd
import numpy as np
import os
import warnings
from pathlib import Path
from config.constants import FINANCE_FEE_PATH

warnings.filterwarnings("ignore")


def extract_transform_chdr(file_path=FINANCE_FEE_PATH, file_name='S&D.xlsx'):
    chdr = pd.read_excel(Path(file_path)/file_name, sheet_name="CHDR")

    chdr.columns = chdr.columns.str.lower().str.replace(r"[()/ ]", "_", regex=True)

    chdr.rename(columns={
        'schemes_types': "bursary_deduction",
        'tuition_fee_waiver__%_': 'snd_rate',
        'original_c3__before_amortized_' : 'c3',
        'total_waiver__exclude_tuition_fee_': 'flat_waiver'
    }, inplace=True)

    # Create new columns

    chdr = chdr.assign(campus="TU", snd_type="CHDR")

    # drop master__id column
    chdr.drop(columns=['master___phd'], inplace=True)
    
    return chdr


def extract_transform_snd(file_path = FINANCE_FEE_PATH, file_name = 'S&D.xlsx'):
    snd = pd.read_excel(Path(file_path)/file_name, sheet_name="MarComm")

    # reformat column names
    snd.columns = snd.columns.str.lower().str.replace(r"[()/ ]", "_", regex=True)
    snd['intake_year'] = snd['intake_year'].astype('Int64')  # Fill NaN values with 0 before converting to int

    # Renaming the first three columns and reformatting column names
    snd.rename(columns={
        "#": "snd_no",
        'c3': 'snd_amortized',
        'original_c3__before_amortized_' : 'c3',
        'type_of_s&d': 'snd_type',
        'institutions': 'campus'
    }, inplace=True, errors='ignore')

    # Melt the dataframe based on the intake_cycle
    id_vars = ['snd_no', 'bursary_deduction', 'bursary_group', 'guideline',
               'intake_year', 'snd_type', 'full_scholarship', 'remarks_changes', 'campus', 'snd_amortized']
    snd = snd.melt(id_vars=id_vars, var_name="intake_cycle", value_name="snd_amount")

    # format the string columns
    cols_to_strip = ['intake_cycle', 'bursary_deduction', 'snd_type', 'campus']
    snd[cols_to_strip] = snd[cols_to_strip].apply(lambda x: x.str.strip().str.upper() if x.name == 'intake_cycle' else x.str.strip())

    # Apply the conditions to create new columns
    snd['snd_amount'] = pd.to_numeric(snd['snd_amount'], errors='coerce').fillna(0)

    # Non-amortized S&D rate and flat waiver
    snd['snd_rate'] = np.where(snd['snd_amount'] <= 1, snd['snd_amount'], 0) # Percentage
    snd['flat_waiver'] = np.where(snd['snd_amount'] > 1, snd['snd_amount'], 0) # Absolute amount

    # Amortized S&D rate and flat waiver
    snd['snd_rate_amortized'] = np.where(snd['snd_amortized'] <= 1, snd['snd_amortized'], 0) # Percentage
    snd['flat_waiver_amortized'] = np.where(snd['snd_amortized'] > 1, snd['snd_amortized'], 0) # Absolute amount

    rel_cols = ['bursary_deduction', 'bursary_group', 'full_scholarship', 'intake_year', 'intake_cycle', 'campus', 
                'snd_type', 'snd_rate', 'flat_waiver', 'snd_rate_amortized', 'flat_waiver_amortized']
    
    return snd[rel_cols]

def preprocess_snd():
    # Load and preprocess the S&D data
    snd = (
        extract_transform_snd()
        .pipe(lambda df: pd.concat([df, extract_transform_chdr()], axis=0, ignore_index=True))
    )
    
    return snd