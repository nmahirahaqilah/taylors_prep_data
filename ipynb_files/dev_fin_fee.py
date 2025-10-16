import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import warnings
from pathlib import Path
from config.constants import FINANCE_FEE_PATH
from r2r_pipelines.utils import assign_intake_cycle, create_pg_connection

warnings.filterwarnings("ignore")

## International total fees dataset
def transform_fees_by_segment(file_path = FINANCE_FEE_PATH, 
                            file_name = "TU+TC Total Tuition Fees by Segment.xlsx", 
                            sheet_name = 'TU'):
    # Read the excel file
    df = pd.read_excel(Path(file_path)/file_name, sheet_name=sheet_name, header=5)

    # Renaming the first three columns and reformatting column names
    df.rename(columns={
        "Unnamed: 0": "prog_name",
        "Unnamed: 1": "intake",
        "Unnamed: 2": "intake_semester"
    }, inplace=True)
    
    df.columns = df.columns.str.lower().str.replace(r"[() ]", "_", regex=True)

    # Keep only rows with "semester" in the 'intake_semester' column
    df = df[df['intake_semester'].str.contains("Semester", na=False, case=False)]

    # Remove the 'INACTIVE' string from the 'prog_name' column
    df['prog_name'] = df['prog_name'].str.replace(r'\(INACTIVE\)|- INACTIVE', '', regex=True).str.strip()

    # Reformat the 'intake_semester' column to only contain the month integer
    df['intake_semester'] = df['intake_semester'].str.extract(r'(\d+)').astype(int)
    
    return df

def extract_transform_fees_by_segment():
    df = pd.concat(
        [transform_fees_by_segment(sheet_name=sheet) for sheet in ['TU', 'TC']], ignore_index=True
        ).rename(columns={'total_tuition_fees__local_': 'total_tuition_fees_local',
                          'total_tuition_fee_per_student__international_': 'total_tuition_fees_international'})\
        .reset_index(drop=True)
    
    relevant_cols = ['prog_name', 'intake', 'intake_semester', 
                     'total_tuition_fees_local', 'total_tuition_fees_international']
    df = df[relevant_cols]
    
    return df

## Academic calendar dataset
def transform_acad_calendar(file_path = FINANCE_FEE_PATH,
                             file_name = "TUSB and TMSB - TM1 Acad Calendar.xlsx",
                             sheet_name = 'TUSB'):
    df = pd.read_excel(Path(file_path)/file_name, sheet_name=sheet_name, header=5)

    # Renaming the first three columns and reformatting column names
    df.rename(columns={
        "Unnamed: 0": "prog_name",
        "Unnamed: 1": "intake",
        "Unnamed: 2": "intake_semester"
    }, inplace=True)

    df.columns = df.columns.str.lower().str.replace(r"[() ]", "_", regex=True)

    # Keep only rows with "semester" in the 'intake_semester' column
    df = df[df['intake_semester'].str.contains("Semester", na=False, case=False)]

    # Remove the 'INACTIVE' string from the 'prog_name' column
    df['prog_name'] = df['prog_name'].str.replace(r'\(INACTIVE\)|- INACTIVE', '', regex=True).str.strip()

    # Reformat the 'intake_semester' column to only contain the month integer
    df['intake_semester'] = df['intake_semester'].str.extract(r'(\d+)').astype(int)

    return df

def extract_transform_acad_calendar():
    df = pd.concat(
        [transform_acad_calendar(sheet_name=sheet) for sheet in ['TUSB', 'TMSB']], ignore_index=True
        ).reset_index(drop=True)
    
    # Convert start_month and end_month to datetime format
    df['start_month'] = pd.to_datetime(df['start_month'], errors='coerce', format='%b-%y')
    df['end_month'] = pd.to_datetime(df['end_month'], errors='coerce', format='%b-%y')
        
    relevant_cols = ['prog_name', 'intake', 'intake_semester', 'start_month', 'end_month']
    df = df[relevant_cols]
        
    return df

## CALSACE table
def extract_transform_calsace(file_path = FINANCE_FEE_PATH, file_name = "BI_Extract_TMStudentPercent_TC.csv"):
    df = pd.read_csv(Path(file_path)/file_name)

    # Rename columns
    rename_dict = {
        "ProgrammeName": 'prog_name',
        "Intake": 'intake',
        'StudentType': 'student_type',
        '%CAL4Subjects': 'cal_4_subjects', 
        '%1ScienceSubject': 'perc_1_science_subject', 
        '%2ScienceSubject': 'perc_2_science_subject'
    }
    df.rename(columns=rename_dict, inplace=True)

    # Filter rows and select relevant columns
    df = df[df['4DigitsCode'].isin(['CALH', 'SAMH'])].iloc[:, 2:]
    
    # Remove the 'INACTIVE' string from the 'prog_name' column
    df['prog_name'] = df['prog_name'].str.replace(r'\(INACTIVE\)|- INACTIVE', '', regex=True).str.strip()
    
    # Calculate calsace fees multipliers
    df['calsace_fee_mult_loc'] = np.where(df['student_type'] == 'New - Local', df['cal_4_subjects'] * 1/3, 0)
    df['calsace_fee_mult_intl'] = np.where(df['student_type'] == 'New - International', df['cal_4_subjects'] * 1/3, 0)
    df['calsace_sci_fee_mult_loc'] = np.where(df['student_type'] == 'New - Local', df['perc_1_science_subject'] + 2 * df['perc_2_science_subject'], 0)
    df['calsace_sci_fee_mult_intl'] = np.where(df['student_type'] == 'New - International', df['perc_1_science_subject'] + 2 * df['perc_2_science_subject'], 0)
    
    # Finalize columns
    final_cols = ['prog_name', 'intake',
                  'calsace_fee_mult_loc', 'calsace_fee_mult_intl',
                  'calsace_sci_fee_mult_loc', 'calsace_sci_fee_mult_intl']

    df = df[final_cols]
    
    df = df.groupby(['prog_name', 'intake']).agg({
            'calsace_fee_mult_loc': 'max',
            'calsace_fee_mult_intl': 'max',
            'calsace_sci_fee_mult_loc': 'max',
            'calsace_sci_fee_mult_intl': 'max'
        }).reset_index()
    
    return df

def extract_fin_fees_pgsql():
    fin_fee_query = """SELECT * FROM r2r_finance_fees"""
    engine = create_pg_connection()
    
    with engine.connect() as connection:
        df = pd.read_sql_query(fin_fee_query, connection)
    print("Data loaded successfully from cms_sas database")
    return df

def extract_fin_fees_manual(file_path = FINANCE_FEE_PATH, file_name = "E_FinanceFee_manual.xlsx"):
    # Read the excel file
    return pd.read_excel(Path(file_path)/file_name, sheet_name="C_FinanceFee", header=0)

def extract_transform_fin_fees():
    fin_df = extract_fin_fees_pgsql()
    
    # Renaming columns
    rename_dict = {
        'course_desc_tm1': "prog_name",
        "course_desc_jarvis": "prog_name_jarvis",
        "intake": "intake",
        'semester': 'intake_semester',
        "year": "intake_year",
        'int_enrollment_fee': 'intl_enrollment_fee',
        'int_student_charges': 'intl_student_charges',
        'int_annual_fee': 'intl_annual_fee',
        'tmsciencefee': 'calsace_science_fee'
    }
    fin_df.rename(columns=rename_dict, inplace=True)

    # Select relevant columns
    fin_relevant_cols = ['prog_name', 'prog_name_jarvis', 'intake', 'intake_semester', 'intake_year', 'campus', 
                         'start_date', 'end_date', 'attrition', 'cms_progcode',
                         'intl_enrollment_fee', 'intl_student_charges', 'intl_annual_fee', 'int_total_fee',
                         'loc_enrollment_fee', 'loc_resource_fee', 'loc_tuition_fee', 'calsace_science_fee']
    fin_df = fin_df[fin_relevant_cols]
    
    # Remove the 'INACTIVE' string from the 'prog_name' column
    fin_df['prog_name'] = fin_df['prog_name'].str.replace(r'\(INACTIVE\)|- INACTIVE', '', regex=True).str.strip()
    
    # Keep rows with intake_month 202001 onwards
    fin_df = fin_df[fin_df['intake'] > 201900].reset_index(drop=True)
    
    # Remove duplicates from the prog_name, intake_month, intake_semester columns
    fin_df = fin_df.drop_duplicates(subset=['prog_name', 'intake', 'intake_semester'])
    
    return fin_df

def preprocess_finance_fees():
    # load data
    print("Start preprocessing finance fee files...")
    print("Loading data...")
    total_fees_df = extract_transform_fees_by_segment()
    calsace_df = extract_transform_calsace()
    acadcalendar_df = extract_transform_acad_calendar()
    fin_df = extract_transform_fin_fees()
    
    print("Merging 'international total fees' data...")
    # Merge fin_df and total_fees_df on prog_name, intake_month, and intake_semester
    fin_merged = fin_df.merge(total_fees_df, on=['prog_name', 'intake', 'intake_semester'], how='left')
    
    # Remove duplicates based on prog_name, intake_month, and intake_semester
    fin_merged.drop_duplicates(subset=['prog_name', 'intake', 'intake_semester'], inplace=True)

    # Calculate total tuition fee based on the total fees file (intl)
    fin_merged['loc_tuition_fee'] = fin_merged['total_tuition_fees_local'].fillna(fin_merged['loc_tuition_fee'])
    fin_merged['intl_tuition_fee'] = fin_merged['total_tuition_fees_international'].fillna(fin_merged['loc_tuition_fee'])

    print("Merging 'academic calendar' data...")
    # Merge with academic calendar to obtain the academic start and end dates
    fin_merged = fin_merged.merge(acadcalendar_df, on=['prog_name', 'intake', 'intake_semester'], how='left')

    fin_merged['acad_start_date'] = fin_merged['start_date'].fillna(fin_merged['start_month'])
    fin_merged['acad_end_date'] = fin_merged['end_date'].fillna(fin_merged['end_month'])

    # Convert acad_start_date and acad_end_date to datetime format
    fin_merged['acad_start_date'] = pd.to_datetime(fin_merged['acad_start_date'], errors='coerce')
    fin_merged['acad_end_date'] = pd.to_datetime(fin_merged['acad_end_date'], errors='coerce')

    print("Merging 'CALSACE' data...")
    # Merge with calsace data
    fin_merged = fin_merged.merge(calsace_df, on=['prog_name', 'intake'], how='left')

    # Split intake into intake_month and intake_year
    fin_merged['intake_month'] = fin_merged['intake'] % 100
    fin_merged['intake_year'] = fin_merged['intake'] // 100

    # Create intake_cycle column
    fin_merged['intake_cycle'] = assign_intake_cycle(fin_merged, 'intake_month')

    print("Calculating amortization related columns...")
    # Create amortized_nom column
    fin_merged['amortized_nom'] = np.where(
        fin_merged['acad_start_date'].dt.year != fin_merged['acad_end_date'].dt.year,
        (12 - fin_merged['acad_start_date'].dt.month) + 12 * (fin_merged['acad_end_date'].dt.year - fin_merged['acad_start_date'].dt.year - 1) + 1,
        (fin_merged['acad_end_date'].dt.month - fin_merged['acad_start_date'].dt.month) + 1
    )

    # Calculate the amortized_denom
    fin_merged['amortized_denom'] = (
        (fin_merged['acad_end_date'].dt.year - fin_merged['acad_start_date'].dt.year) * 12 +
        fin_merged['acad_end_date'].dt.month - fin_merged['acad_start_date'].dt.month + 1
    )

    # Finalize columns
    fin_cols = [
        'prog_name', 'intake', 'intake_semester', 'intake_month', 'intake_cycle', 'intake_year', 'campus',
        'acad_start_date', 'acad_end_date', 'attrition',
        'intl_enrollment_fee', 'intl_student_charges', 'intl_annual_fee', 'intl_tuition_fee',
        'loc_enrollment_fee', 'loc_resource_fee', 'loc_tuition_fee', 
        'calsace_science_fee', 'calsace_fee_mult_loc', 'calsace_fee_mult_intl',
        'calsace_sci_fee_mult_loc', 'calsace_sci_fee_mult_intl', 'amortized_nom', 'amortized_denom'
    ]
    print("Preprocessing finance fee files completed.")
    
    return fin_merged[fin_cols]


def calculate_first_year_fee():
    fin_fee = preprocess_finance_fees()
    
    # Select only the rows where the academic start date year is equal to the intake year, as we are calculating only the first year fees
    fee_by_cycle = fin_fee[(fin_fee['acad_start_date'].dt.year == fin_fee['intake_year'])].reset_index(drop=True)

    # Amortization formula is only applicable from 2023 onwards
    fee_by_cycle['amortized_nom'] = fee_by_cycle.apply(
        lambda row: row['amortized_nom'] if row['intake_year'] >= 2023 else row['amortized_denom'], axis=1
    )

    # Calculate first_year_fee by grouping the data by prog_name, campus, intake_year, intake_cycle, and intake
    first_year_fee = fee_by_cycle.groupby(['prog_name', 'campus', 'intake_year', 'intake_cycle', 'intake']).apply(
        lambda df: pd.Series({
            'fee_period_start': df['acad_start_date'].min(),
            'fee_period_end': df['acad_end_date'].max(),
            # Local Fees
            'loc_non_tuition_fees_actual': (df['loc_enrollment_fee'] + df['loc_resource_fee'] + 
                                            df['calsace_sci_fee_mult_loc'].fillna(0) * df['calsace_science_fee']).sum(),
            'loc_tuition_fees_actual': (df['loc_tuition_fee'] + df['loc_tuition_fee'] * 
                                                df['calsace_fee_mult_loc'].fillna(0)).sum(),
            # Local fees adjusted for amortization and attrition
            'loc_non_tuition_fees_adj': ((df['loc_enrollment_fee'] + df['loc_resource_fee'] + 
                                                df['calsace_sci_fee_mult_loc'].fillna(0) * df['calsace_science_fee']) * 
                                                df['amortized_nom'] / df['amortized_denom'] * (1-df['attrition'])).sum(),
            'loc_tuition_fees_adj': ((df['loc_tuition_fee'] + df['loc_tuition_fee'] * 
                                    df['calsace_fee_mult_loc'].fillna(0)) * 
                                    df['amortized_nom'] / df['amortized_denom'] * (1-df['attrition'])).sum(),
            # International Fees
            'intl_non_tuition_fees_actual': (df['intl_enrollment_fee'] + df['loc_resource_fee'] + 
                                            df['intl_student_charges'] + df['intl_annual_fee'] + 
                                            df['calsace_sci_fee_mult_intl'].fillna(0) * df['calsace_science_fee']).sum(),
            'intl_tuition_fees_actual': (df['intl_tuition_fee'] + df['intl_tuition_fee'] * 
                                        df['calsace_fee_mult_intl'].fillna(0)).sum(),
            # International fees adjusted for amortization and attrition
            'intl_non_tuition_fees_adj': ((df['intl_enrollment_fee'] + df['loc_resource_fee'] + 
                                        df['intl_student_charges'] + df['intl_annual_fee'] + 
                                        df['calsace_sci_fee_mult_intl'].fillna(0) * df['calsace_science_fee']) * 
                                        df['amortized_nom'] / df['amortized_denom'] * (1-df['attrition'])).sum(),
            'intl_tuition_fees_adj': ((df['intl_tuition_fee'] + df['intl_tuition_fee'] * 
                                    df['calsace_fee_mult_intl'].fillna(0)) * 
                                    df['amortized_nom'] / df['amortized_denom'] * (1-df['attrition'])).sum(),
            # amortization flags     
            'amortized_nom': df['amortized_nom'].sum(),
            'amortized_denom': df['amortized_denom'].sum(),
        })
    ).reset_index()
    
    return first_year_fee


def preprocess_first_year_fee():
    first_year_fee = calculate_first_year_fee()    
    
    # Unpivot first_year_fee to get long format
    first_year_fee_long = pd.melt(
        first_year_fee, 
        id_vars=['prog_name', 'campus', 'intake_year', 'intake_cycle', 'intake', 'fee_period_start', 'fee_period_end', 'amortized_nom', 'amortized_denom'],
        var_name='fee_type', 
        value_name='fee_amount'
    )

    # Create a new column 'market_segment' based on the fee_type column
    first_year_fee_long['market_segment'] = np.where(
        first_year_fee_long['fee_type'].str.contains('loc'), 'Domestic', 'International'
    )

    # Append 'Progression' market segment for Domestic rows
    progression_df = first_year_fee_long[first_year_fee_long['market_segment'] == 'Domestic'].copy()
    progression_df['market_segment'] = 'Progression'
    first_year_fee_long = pd.concat([first_year_fee_long, progression_df], ignore_index=True)

    # remove the 'loc_' and 'intl_' prefixes from the fee_type column
    first_year_fee_long['fee_type'] = first_year_fee_long['fee_type'].str.replace(r'loc_|intl_', '', regex=True)

    # Pivot the dataframe to get the final output
    first_year_fee_long = first_year_fee_long.pivot_table(
        index=['prog_name', 'campus', 'intake_year', 'intake_cycle', 'intake', 'fee_period_start', 'fee_period_end', 'market_segment', 'amortized_nom', 'amortized_denom'], 
        columns=['fee_type'], 
        values='fee_amount'
        ).reset_index()

    # Use vectorized comparison for is_amortized
    first_year_fee_long['is_amortized'] = first_year_fee_long['amortized_nom'].ne(first_year_fee_long['amortized_denom'])
    
    return first_year_fee_long