import numpy as np
import pandas as pd
import os
import warnings
from pathlib import Path
from config.constants import TM1_ANNUAL_PATH, CLEAN_DATA_PATH

# Ignore warnings
warnings.filterwarnings("ignore")

# Set the pandas option to opt-in to the future behavior
pd.set_option('future.no_silent_downcasting', True)

def extract_transform_population(file_path = TM1_ANNUAL_PATH, file_name = "TM1_Total_Student_Population.xlsx"):
    print("Processing Total Student Population file...")
    df = pd.read_excel(Path(file_path)/file_name, sheet_name="Total_Student_Population", header=None)
    
    # Transform the data to a long format, and make the first row as the header
    df = df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Rename the first four columns
    df.columns = ['campus', 'data_type', 'field_name', 'month'] + list(df.columns[4:])

    # Melt the fifth column onwards
    df = df.melt(id_vars=['campus', 'data_type', 'field_name', 'month'], 
                 var_name='prog_name_tm1', 
                 value_name='value')

    # Remove rows where "month" does not contain Dec
    df = df[df['month'].str.contains("Dec", na=False)]
    df['month'] = pd.to_datetime(df['month'], format='%b-%y')
    df['year'] = df['month'].dt.year

    # Remove the string (@ ME) from the field_name column
    df['field_name'] = df['field_name'].str.replace(r"\(@ ME\)", "", regex=True)
    df[['acc_name_tm1', 'field_name_tm1']] = df['field_name'].str.split(' - ', expand=True, n=1)

    # Remove unwanted programs
    df = df[~df['prog_name_tm1'].str.contains("All Programs and Products", na=False)]

    # Finalize table
    return df[['campus', 'year', 'field_name_tm1', 'prog_name_tm1', 'value']].reset_index(drop=True)


def extract_transform_exclusion(file_path = TM1_ANNUAL_PATH, file_name = "TM1_Exclusion.xlsx"):
    print("Processing Exclusion file...")
    main_df = pd.read_excel(Path(file_path)/file_name, sheet_name="Exclusion", header=None)
    
    # Transform the data to a long format, and make the first row as the header
    df = main_df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Rename the first four columns
    df.columns = ['campus', 'type', 'field_name', 'year'] + list(df.columns[4:])

    # Drop unnecessary columns if they exist
    df.drop(columns=[col for col in ['All Programs and Products', 'type'] if col in df.columns], errors='ignore')

    # Melt the fifth column onwards
    df = df.melt(id_vars=['campus', 'field_name', 'year'], 
                    var_name='prog_name_tm1', 
                    value_name='value')

    # Remove rows from "year" to retain only the FY rows
    df = df[df['year'].str.contains("FY")]
    df['year'] = df['year'].str.replace("FY ", "").astype(int)

    # Split and expand the field_name column
    df[['acc_name_tm1', 'field_name_tm1']] = df['field_name'].str.split(' - ', expand=True, n=1)
    # Keep only the rows with 'field_name_tm1' containing 1) 'ACADEMIC RELATED REVENUE' or 2) 'NON ACADEMIC RELATED REVENUE'
    df = df[df['field_name_tm1'].str.contains("ACADEMIC RELATED REVENUE|NON ACADEMIC RELATED REVENUE")]

    # Finalize table
    return df[['campus', 'year', 'field_name_tm1', 'prog_name_tm1', 'value']].reset_index(drop=True)


def transform_fin_efts(main_df):
    # Transform the data to a long format, and make the first row as the header
    df = main_df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Rename the first four columns
    df.columns = ['campus', 'data_type', 'field_name', 'year'] + list(df.columns[4:])

    # Melt the fifth column onwards
    df = df.melt(id_vars=['campus', 'data_type', 'field_name', 'year'], 
                 var_name='prog_name_tm1', 
                 value_name='value')

    # Remove rows from "year" to retain only the FY rows
    df = df[df['year'].str.contains("FY")]
    df['year'] = df['year'].str.replace("FY ", "").astype(int)

    # Remove unwanted programs
    df = df[~df['prog_name_tm1'].isin(["All Programs and Products", "Common Programme"])]

    # Split and expand the field_name column
    df[['acc_name_tm1', 'field_name_tm1']] = df['field_name'].str.split(' - ', expand=True, n=1)

    # Finalize table
    return df[['campus', 'year', 'field_name_tm1', 'prog_name_tm1', 'value']].reset_index(drop=True)


def extract_transform_efts(file_path = TM1_ANNUAL_PATH, file_name = "TM1_EFTS.xlsx"):
    print("Processing EFTS File...")
    efts_df = pd.read_excel(Path(file_path)/file_name, sheet_name="EFTS", header=None)

    return transform_fin_efts(efts_df)


def extract_transform_financial(file_path = TM1_ANNUAL_PATH, file_name = "TM1_Revenue.xlsx"):
    # Process annual financial data
    sheet_names = ['Gross_Revenue', 'Net_Revenue', 'PBT']

    for sheet_name in sheet_names:
        print(f"Processing {sheet_name} File...")
        financial_df = pd.read_excel(Path(file_path)/file_name, sheet_name=sheet_name, header=None)
        
        # save the cleaned data to the sheet_name dataframe
        globals()[sheet_name.lower() + "_df"] = transform_fin_efts(financial_df)
        
    gross_revenue_df = globals().get('gross_revenue_df')
    net_revenue_df = globals().get('net_revenue_df')
    pbt_df = globals().get('pbt_df')
    
    return gross_revenue_df, net_revenue_df, pbt_df


def replace_gross_revenue(main_df, ex_df, ex_prog, metric):
    """
    Replace the "GROSS REVENUE" values in the main TM1 data with the "ACADEMIC RELATED REVENUE" from the exclusion data
    """
    hub_df = main_df[(main_df['prog_name_tm1'].isin(ex_prog)) & (main_df['field_name_tm1'] == metric)]
    spoke_df = ex_df[ex_df['field_name_tm1'] == 'ACADEMIC RELATED REVENUE']

    hub_df = hub_df.merge(spoke_df[['campus', 'year', 'prog_name_tm1', 'value']], 
                          on=['campus', 'year', 'prog_name_tm1'], 
                          suffixes=('_hub', '_spoke'),
                          how='left').fillna(0).infer_objects(copy=False)

    hub_df['value'] = hub_df['value_spoke']

    # Replace the values in the main data
    main_df.loc[(main_df['prog_name_tm1'].isin(ex_prog)) & (main_df['field_name_tm1'] == metric), 'value'] = hub_df['value'].values
    
    
def deduct_non_academic_revenue(main_df, ex_df, ex_prog, metric):
    """
    Deduct the "NON-ACADEMIC RELATED REVENUE" values from the "ACADEMIC RELATED REVENUE" values in the main TM1 data
    """

    # filter the excluded programme from the main data
    hub_df = main_df[(main_df['prog_name_tm1'].isin(ex_prog)) & (main_df['field_name_tm1'] == metric)]
    spoke_df = ex_df[ex_df['field_name_tm1'] == 'NON-ACADEMIC RELATED REVENUE']

    hub_df = hub_df.merge(spoke_df[['campus', 'year', 'prog_name_tm1',  'value']], 
                        on=['campus', 'year', 'prog_name_tm1'], 
                        suffixes=('_hub', '_spoke'),
                        how='left').fillna(0).infer_objects(copy=False)

    hub_df['value'] = hub_df['value_hub'] - hub_df['value_spoke']

    # Replace the values in the main data
    main_df.loc[(main_df['prog_name_tm1'].isin(ex_prog)) & (main_df['field_name_tm1'] == metric), 'value'] = hub_df['value'].values
    
    
def preprocess_annual_data():
    try:
        population_df = extract_transform_population()
        efts_df = extract_transform_efts()
        gross_revenue_df, net_revenue_df, pbt_df = extract_transform_financial()
        ex_df = extract_transform_exclusion()
        
        main_df = pd.concat([population_df, efts_df, gross_revenue_df, net_revenue_df, pbt_df])
        
        # programme involved for exclusion calculation, save as list
        ex_prog = ex_df['prog_name_tm1'].unique().tolist()
        
        # Replace Gross revenue with Academic Related Revenue
        replace_gross_revenue(main_df, ex_df, ex_prog, metric='GROSS REVENUE')
        
        # Deduct non_academic_revenue from Net Revenue and PBT to be consistent with the exclusion data 
        deduct_non_academic_revenue(main_df, ex_df, ex_prog, metric='NET REVENUE')
        deduct_non_academic_revenue(main_df, ex_df, ex_prog, metric='PROFIT BEFORE TAX')
        
        # white space removal from fied_name_tm1
        main_df['field_name_tm1'] = main_df['field_name_tm1'].str.strip()
        
        print("All files have been processed, respectively.")
        return main_df

    except Exception as e:
        print(f"Error processing and saving data: {e}")