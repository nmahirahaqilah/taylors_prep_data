import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

from r2r_pipelines import export_db
from config.constants import ANNUAL_TARGET_PATH
from r2r_pipelines.prep_pg_enreg import assign_intake_cycle


def process_annual_target_data(file_name, intake_year, annual_target_path=ANNUAL_TARGET_PATH):
    targets = pd.read_excel(annual_target_path + '/' + file_name, sheet_name='TUTC target', header=1)
    targets.rename(columns={'Prog_Code':'prog_code', 'Prog_Name': 'prog_name', 'Unnamed: 2':'market_segment', 'Unnamed: 20':'target_type'}, inplace=True)
    targets = targets.iloc[:, :21]
    targets = targets[targets['prog_name'].notnull()].reset_index(drop=True)
    targets['prog_code'] = targets['prog_code'].astype(str)

    # Convert to long format
    targets = targets.melt(id_vars=['prog_code', 'prog_name', 'market_segment', 'target_type'], var_name='intake_month', value_name='enreg_target')
    
    # fill in missing values in the enreg_target column with 0
    targets['enreg_target'] = targets['enreg_target'].fillna(0)
    targets['enreg_target'] = targets['enreg_target'].astype(int)
    
    # Create a new column for intake year
    targets['intake_year'] = intake_year

    # remove rows that have "Total" in the intake_month column
    targets = targets[~targets['intake_month'].astype(str).str.contains('Total')].reset_index(drop=True)

    # Replace "Advanced March" with the correct intake month
    targets['intake_month'] = targets['intake_month'].astype(str).str.replace('Advanced March', intake_year + '03').str.strip()

    # Create a new column for intake cycle
    targets['month'] = pd.to_numeric(targets['intake_month'], errors='coerce') % 100
    targets['intake_cycle'] = assign_intake_cycle(targets, column_name='month')
    targets.drop(columns=['month'], inplace=True)
    
    return targets

def adjust_2021_targets(ann_tgt_df):
    adj_21 = ann_tgt_df[ann_tgt_df['intake_year'] == '2021'
                        ].pivot_table(index=['prog_code', 'prog_name', 'market_segment', 'intake_year', 'intake_cycle', 'intake_month'], 
                                        columns='target_type', values='enreg_target', aggfunc='sum').reset_index()
                    
    adj_21['Budget'] = adj_21.apply(lambda row: row['Base'] if row['intake_cycle'] in ['C1', 'C2'] else row['Worst'], axis=1)

    adj_21 = adj_21.melt(id_vars=['prog_code', 'prog_name', 'market_segment', 'intake_year', 'intake_cycle', 'intake_month'],
                var_name='target_type', value_name='enreg_target')

    adj_21 = adj_21[(adj_21['target_type'] == 'Budget') & (adj_21['intake_year'] == '2021')].reset_index(drop=True)

    return pd.concat([ann_tgt_df, adj_21[(adj_21['target_type'] == 'Budget') & (adj_21['intake_year'] == '2021')]], ignore_index=True)

def preprocess_annual_targets(annual_target_path=ANNUAL_TARGET_PATH):
    ann_tgt_df = pd.DataFrame()

    # create a list of all files in the test folder
    files = os.listdir(annual_target_path)

    for file_name in files:
        if file_name.endswith('.xlsx'):
            print('Processing file:', file_name)
            intake_year = file_name.split('.')[0].split('_')[2]
            targets = process_annual_target_data(file_name, intake_year)
            
            ann_tgt_df = pd.concat([ann_tgt_df, targets], ignore_index=True)

    adj_ann_tgt = adjust_2021_targets(ann_tgt_df)

    adj_ann_tgt['intake_year'] = adj_ann_tgt['intake_year'].astype(int)

    engine = export_db.marcommdb_connection()
    adj_ann_tgt.to_sql('annual_targets', engine, schema='public', if_exists='replace', index=False)
    
    return adj_ann_tgt

