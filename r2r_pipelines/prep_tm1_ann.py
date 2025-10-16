import numpy as np
import pandas as pd
import os
from config.constants import TM1_ANNUAL_PATH, CLEAN_DATA_PATH

# Set the pandas option to opt-in to the future behavior
pd.set_option('future.no_silent_downcasting', True)


# Function to clean student population data from TM1
def clean_population_data(main_df):
    # Transform the data to a long format, and make the first row as the header
    df = main_df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Rename the first four columns
    df.columns = ['campus', 'data_type', 'field_name', 'month'] + list(df.columns[4:])

    # Melt the data from the fifth column onwards
    df = df.melt(id_vars=['campus', 'data_type', 'field_name', 'month'], 
                 var_name='prog_name_tm1', 
                 value_name='value')

    # Filter and clean the data
    df = df[df['month'].str.contains("Dec")]
    df['field_name'] = df['field_name'].str.replace("(@ ME)", "")
    df = df[~df['prog_name_tm1'].str.contains("All Programs and Products")]
    df['month'] = pd.to_datetime(df['month'], format='%b-%y')
    df['year'] = df['month'].dt.year
    
    # Split and expand the field_name column
    df[['acc_name_tm1', 'field_name_tm1']] = df['field_name'].str.split(' - ', expand=True, n=1)

    # Finalize table
    df = df[['campus', 'year', 'field_name_tm1', 'prog_name_tm1', 'value']]\
        .reset_index(drop=True)
    
    return df


# Function to clean the annual financial data from TM1
def clean_financial_data(main_df):
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

    # Filter and clean the data
    df = df[df['year'].str.contains("FY")]
    df['year'] = df['year'].str.replace("FY ", "").astype(int)

    # Filter the DataFrame to remove rows containing the specified strings
    df['prog_name_tm1'] = df['prog_name_tm1'].astype(str)
    df = df[~df['prog_name_tm1'].str.contains("All Programs and Products|Common Programme")]

    # Split and expand the field_name column
    df[['acc_name_tm1', 'field_name_tm1']] = df['field_name'].str.split(' - ', expand=True, n=1)

    # Finalize table
    df = df[['campus', 'year', 'field_name_tm1', 'prog_name_tm1', 'value']].reset_index(drop=True)
    
    return df


# Function to clean the annual financial data from TM1
def clean_exclusion_data(main_df): 
    # Transform the data to a long format, and make the first row as the header
    df = main_df.T

    # Make the first row as the header, and drop the first row
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Rename the first four columns
    df.columns = ['campus', 'type', 'field_name', 'year'] + list(df.columns[4:])

    try:
        df.drop(columns='All Programs and Products', inplace=True)
        df.drop(columns='type', inplace=True)
    except Exception as e:
        print(f"Error dropping columns: {e}")

    # Melt the fifth column onwards
    df = df.melt(id_vars=['campus', 'field_name', 'year'], 
                    var_name='prog_name_tm1', 
                    value_name='value')

    # Remove rows from "year" to retain only the FY rows
    df = df[df['year'].str.contains("FY")]

    # Remove "FY" from the year column
    df['year'] = df['year'].str.replace("FY ", "").astype(int)

    # Split and expand the field_name column
    df[['acc_name_tm1', 'field_name_tm1']] = df['field_name'].str.split(' - ', expand=True, n=1)

    # Finalize table
    df = df[['campus', 'year', 'field_name_tm1', 'prog_name_tm1', 'value']].reset_index(drop=True)
    
    # Keep only the rows with 'field_name_tm1' containing 1) 'ACADEMIC RELATED REVENUE' or 2) 'NON ACADEMIC RELATED REVENUE'
    df = df[df['field_name_tm1'].str.contains("ACADEMIC RELATED REVENUE|NON ACADEMIC RELATED REVENUE")]

    return df


# Function to replace the "GROSS REVENUE" values in the main TM1 data with the "ACADEMIC RELATED REVENUE" from the exclusion data
def replace_gross_revenue(main_df, ex_df, ex_prog, metric):
    """
    Replace the "GROSS REVENUE" values in the main TM1 data with 
    the "ACADEMIC RELATED REVENUE" from the exclusion data
    """
    hub_df = main_df[(main_df['prog_name_tm1'].isin(ex_prog)) & (main_df['field_name_tm1'] == metric)]
    spoke_df = ex_df[ex_df['field_name_tm1'] == 'ACADEMIC RELATED REVENUE']

    hub_df = hub_df.merge(spoke_df[['campus', 'year', 'prog_name_tm1', 'value']], 
                          on=['campus', 'year', 'prog_name_tm1'], 
                          suffixes=('_hub', '_spoke'),
                          how='left').fillna(0).infer_objects(copy=False)

    hub_df['value'] = hub_df['value_spoke']

    # Replace the values in the main data
    main_df.loc[(main_df['prog_name_tm1'].isin(ex_prog)) 
                & (main_df['field_name_tm1'] == metric), 'value'] = hub_df['value'].values


# Function to deduct the "NON-ACADEMIC RELATED REVENUE" values from the "ACADEMIC RELATED REVENUE" values in the main TM1 data
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
    main_df.loc[(main_df['prog_name_tm1'].isin(ex_prog)) 
                & (main_df['field_name_tm1'] == metric), 'value'] = hub_df['value'].values


# Function to process the population data from TM1
def process_population_data():
    # Process student population data
    population_df = pd.read_excel(TM1_ANNUAL_PATH + "/TM1_Total_Student_Population.xlsx", 
                                    sheet_name="Total_Student_Population", 
                                    header=None)
    print("Processing Student Population Data...")

    return clean_population_data(population_df)

# Function to process the efts data from TM1
def process_efts_data():
    # Process efts data
    efts_df = pd.read_excel(TM1_ANNUAL_PATH + "/TM1_EFTS.xlsx", 
                            sheet_name="EFTS", 
                            header=None)
    print("Processing EFTS Data...")

    return clean_financial_data(efts_df)

# Function to process the annual financial data from TM1
def process_ann_fin_data():
    # Process annual financial data
    sheet_names = ['Gross_Revenue', 'Net_Revenue', 'PBT']

    for sheet_name in sheet_names:
        print(f"Processing {sheet_name}...")
        financial_df = pd.read_excel(TM1_ANNUAL_PATH + "/TM1_Revenue.xlsx", 
                                        sheet_name=sheet_name, 
                                        header=None)
        
        # save the cleaned data to the sheet_name dataframe
        globals()[sheet_name.lower() + "_df"] = clean_financial_data(financial_df)
        
    gross_revenue_df = globals().get('gross_revenue_df')
    net_revenue_df = globals().get('net_revenue_df')
    pbt_df = globals().get('pbt_df')
    
    return gross_revenue_df, net_revenue_df, pbt_df

# Function to process the exclusion data from TM1
def process_exclusion_data():
    # Process the TM1 Exclusion file
    ex_df = pd.read_excel(TM1_ANNUAL_PATH + "/TM1_Exclusion.xlsx", 
                        sheet_name="Exclusion", 
                        header=None)

    print("Processing Exclusion Data...")
    return clean_exclusion_data(ex_df)

def preprocess_tm1_annual_data():
    try:
        population_df = process_population_data()
        efts_df = process_efts_data()
        gross_revenue_df, net_revenue_df, pbt_df = process_ann_fin_data()
        ex_df = process_exclusion_data()
        
        main_df = pd.concat([population_df, efts_df, gross_revenue_df, net_revenue_df, pbt_df])
        
        # programme involved for exclusion calculation, save as list
        ex_prog = ex_df['prog_name_tm1'].unique().tolist()
        
        # Replace Gross revenue with Academic Related Revenue
        replace_gross_revenue(main_df, ex_df, ex_prog, metric='GROSS REVENUE')
        
        # Deduct non_academic_revenue from Net Revenue and PBT to be consistent with the exclusion data 
        deduct_non_academic_revenue(main_df, ex_df, ex_prog, metric='NET REVENUE')
        deduct_non_academic_revenue(main_df, ex_df, ex_prog, metric='PROFIT BEFORE TAX')

        # save the cleaned data to one excel file
        main_df.to_excel(CLEAN_DATA_PATH + '/cleaned_TM1_consolidated_data.xlsx', index=False)
        #ex_df.to_csv(TEMP_DATA_PATH + 'TM1_exclusion_cleaned.csv', index=False)
        
        print("All files have been processed and saved to the csv files, respectively.")
        
    except Exception as e:
        print(f"Error processing and saving data: {e}")