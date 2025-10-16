import pandas as pd

from pathlib import Path
from config.constants import PRICING_MOHE_PATH
from r2r_pipelines import extract_prog_requirements, assign_prog_labels


def extract_mohe_pricing(file_path = PRICING_MOHE_PATH, file_name = "Redmarch - IPTS Course Fee Database 2024 v151 (updated).xlsx"):
    relevant_columns = ['Group', 'Institution', 'State', 'Region 1', 'Level 1', 'Vertical', 'Specialization',
                    'Course Name (Reformatted)', 'Mode', 'Status', '# Intakes', 'Total Fee']
    
    latest_file = Path(file_path)/file_name
    # Get all sheet names
    sheet_names = pd.ExcelFile(latest_file).sheet_names

    # Filter sheet names that start with "20"
    filtered_sheet_names = [sheet for sheet in sheet_names if sheet.startswith("20")]

    # Read each filtered sheet into a dataframe and store in a dictionary
    dataframes = {sheet: pd.read_excel(latest_file, sheet_name=sheet, usecols=relevant_columns) for sheet in filtered_sheet_names}

    # create a 'cal_year' column for each sheet, taking the year from the sheet name
    for sheet, df in dataframes.items():
        df['cal_year'] = int(sheet[:4])
    
    # Concatenate all dataframes into a single dataframe
    compiled_df = pd.concat(dataframes.values(), ignore_index=True)
    
    rename_columns = {
        'cal_year': 'year',
        'Group': 'group',
        'Institution': 'ipts',
        'State': 'state',
        'Region 1': 'region',
        'Level 1': 'level',
        'Vertical': 'vertical',
        'Specialization': 'specialization',
        'Course Name (Reformatted)': 'course_name_reformatted',
        'Mode': 'study_mode',
        'Status': 'prog_status',
        '# Intakes': 'number_intakes',
        'Total Fee': 'total_fee'
    }
    compiled_df.rename(columns=rename_columns, inplace=True)
    
    # convert values in "specialization", "level", and "vertical" columns to uppercase and remove whitespace
    for col in ['specialization', 'level', 'vertical', 'group']:
        compiled_df[col] = compiled_df[col].astype(str).str.upper().str.strip()
    
    # Replace non-numeric values with NaN
    compiled_df['total_fee'] = pd.to_numeric(compiled_df['total_fee'], errors='coerce')
    
    # if specialization is "PRO ACC - ACCA", then replace 'level' with 'DEG'
    compiled_df.loc[compiled_df['specialization'] == 'PRO ACC - ACCA', 'level'] = 'DEG'
    
    return compiled_df

def preprocess_mohe_pricing():
    px_df = (
        extract_mohe_pricing()
        .pipe(lambda df: assign_prog_labels(df, extract_prog_requirements()))
        .pipe(lambda df: df.assign(
            vertical=df['vertical'].replace({'NAN': 'OTHERS'}),
            specialization=df['specialization'].replace({'NAN': 'OTHERS', 'INVESTIGATION)': 'INVESTIGATION', 'PHARMAC': 'PHARMACY'})))
    )
    
    # Remove rows where 'vertical' contains spaces
    px_df = px_df[~px_df['vertical'].str.contains(' ')]
    
    return px_df