import numpy as np
import pandas as pd
from r2r_pipelines import export_db
import os

from config.constants import CLEAN_DATA_PATH, MAPPING_PATH

def process_enreg_data(folder_path = CLEAN_DATA_PATH, file_name = "/cleaned_cpp_enreg.xlsx"):
    enreg_df = pd.read_excel(folder_path + file_name)
    enreg_df = enreg_df[enreg_df['intake_year'] >= 2024].reset_index(drop=True)
    enreg_df.rename(columns={'ctd_tgt': 'ctd_student'}, inplace=True)
    
    enreg_df = enreg_df\
        .groupby(['reporting_date', 'campus', 'intake_cycle', 'intake_year', 'market_segment', 'tgt_version', 'ctd_tgt_stage'])\
        .agg({'ctd_student': 'sum'})\
        .reset_index()
        
    enreg_df = enreg_df.pivot_table(index=['reporting_date', 'campus', 'intake_cycle', 'intake_year', 'tgt_version', 'ctd_tgt_stage'], 
                        columns='market_segment', 
                        values='ctd_student', 
                        aggfunc='sum')\
        .sort_values(by = ['reporting_date', 'intake_cycle'])\
        .reset_index()

    return enreg_df

def process_nr_data(folder_path = CLEAN_DATA_PATH, file_name = "/cleaned_cpp_nr.xlsx"):
    nr_df = pd.read_excel(folder_path + file_name)
    nr_df = nr_df[nr_df['intake_year'] >= 2024].reset_index(drop=True)

    # Final clean-up
    nr_df = nr_df[['reporting_date', 'intake_cycle', 'intake_year','campus', 'tgt_version', 'ctd_tgt_stage', 'ctd_tgt_gr', 'ctd_tgt_nr']]
    nr_df["ctd_tgt_stage"] = nr_df["ctd_tgt_stage"].replace({'enrollment': 'Enrollment', 'registration': 'Registration'}, regex=True)
    
    return nr_df

def compile_cpp_data():
    cpp_enreg = process_enreg_data()
    cpp_nr = process_nr_data()

    isr_factor = pd.read_excel(MAPPING_PATH + "/isr_fees_premium.xlsx")

    isr_factor = isr_factor[isr_factor['segment'] == 'International']\
        .groupby(['campus', 'intake_cycle', 'enreg'])\
        .agg({'gr_isr_factor': 'max', 'nr_isr_factor': 'max'})\
        .reset_index()\
        .sort_values(by=['enreg', 'campus', 'intake_cycle'])\
        .reset_index(drop=True)
    
    isr_factor.rename(columns={'enreg': 'ctd_tgt_stage'}, inplace=True)

    process_nr = cpp_nr.merge(isr_factor[['campus', 'intake_cycle', 'ctd_tgt_stage', 'gr_isr_factor', 'nr_isr_factor']], 
                              on=['campus', 'intake_cycle', 'ctd_tgt_stage'], 
                              how='inner')

    merged_data = process_nr.merge(cpp_enreg, 
                                   on=['reporting_date', 'campus', 'intake_cycle', 'intake_year', 'tgt_version', 'ctd_tgt_stage'], 
                                   how='left')
    return merged_data

def process_fin_target_by_segment(process_nr, processing_metrics):
    financial_metrics = f'ctd_tgt_{processing_metrics}'
    isr_factor = f'{processing_metrics}_isr_factor'  
    
    process_nr['adj_' + processing_metrics + '_student_Domestic'] = process_nr[financial_metrics] / (process_nr[isr_factor] * process_nr['International'] + process_nr['Domestic'])
    process_nr['adj_' + processing_metrics +'_student_International'] = process_nr['adj_' + processing_metrics +'_student_Domestic'] * process_nr[isr_factor]
    process_nr['ctd_' + processing_metrics + '_Domestic'] = process_nr['Domestic'] * process_nr['adj_' + processing_metrics + '_student_Domestic']
    process_nr['ctd_' + processing_metrics + '_International'] = process_nr['International'] * process_nr['adj_' + processing_metrics + '_student_International']
    
    return process_nr

def consolidate_cpp(process_nr):
    process_nr = process_fin_target_by_segment(process_nr, 'nr')
    process_nr = process_fin_target_by_segment(process_nr, 'gr')
    
    # Final structure
    relevant_cols = ['reporting_date', 'campus', 'intake_cycle', 'intake_year', 'ctd_tgt_stage', 'tgt_version',
                         'Domestic', 'International', 'ctd_gr_Domestic', 'ctd_gr_International', 
                         'ctd_nr_Domestic', 'ctd_nr_International']
    
    cleaned_nr = process_nr[relevant_cols]

    cleaned_nr = cleaned_nr.rename(columns={'Domestic': 'ctd_student_Domestic', 
                                            'International': 'ctd_student_International'})

    cleaned_nr = cleaned_nr.melt(id_vars=['reporting_date', 'campus', 'intake_cycle', 'intake_year', 'ctd_tgt_stage', 'tgt_version'],
                                var_name='metric_segment', 
                                value_name='ctd_target')

    # Split the type column into metric and segment by the second underscore, remove the original metric_segment column
    cleaned_nr[['metric', 'segment']] = cleaned_nr['metric_segment'].str.rsplit('_', n=1, expand=True)
    cleaned_nr = cleaned_nr.drop(columns='metric_segment')

    # final Formatting
    cleaned_nr['ctd_target'] = cleaned_nr['ctd_target'].fillna(0)
    cleaned_nr = cleaned_nr.pivot_table(index=['reporting_date', 'campus', 'intake_cycle', 
                                               'intake_year', 'ctd_tgt_stage', 'segment', 'tgt_version'],
                          columns='metric',
                          values='ctd_target',
                          aggfunc='sum').reset_index()
    
    # add new columns to show the previous week ctd_nr data based on the report_date
    cleaned_nr[['prev_week_ctd_gr', 'prev_week_ctd_nr', 'prev_week_ctd_student']] = cleaned_nr\
        .groupby(['campus', 'intake_cycle', 'intake_year', 'ctd_tgt_stage', 'segment', 'tgt_version'])[['ctd_gr', 'ctd_nr', 'ctd_student']]\
        .shift(1)

    return cleaned_nr

def preprocess_cpp_by_segment():
    cpp_data = compile_cpp_data()
    cleaned_cpp_segment = consolidate_cpp(cpp_data)
    
    cleaned_cpp_segment.rename(columns={'reporting_date': 'reporting_date', 
                                        'campus': 'campus', 
                                        'intake_cycle': 'intake_cycle', 
                                        'intake_year': 'intake_year',
                                        'ctd_tgt_stage': 'ctd_tgt_stage',
                                        'segment': 'market_segment',
                                        'tgt_version': 'tgt_version',
                                        'ctd_gr': 'ctd_tgt_gr',
                                        'ctd_nr': 'ctd_tgt_nr',
                                        'ctd_student': 'ctd_tgt_registration',
                                        'prev_week_ctd_gr': 'lw_ctd_tgt_gr', 
                                        'prev_week_ctd_nr': 'lw_ctd_tgt_nr',
                                        'prev_week_ctd_student': 'lw_ctd_tgt_registration'}, inplace=True)

    cleaned_cpp_segment.to_excel(CLEAN_DATA_PATH + '/cleaned_cpp_segment.xlsx', index=False)

    engine = export_db.marcommdb_connection()
    cleaned_cpp_segment.to_sql('cpp_segment', engine, schema='public', if_exists='replace', index=False)

    return cleaned_cpp_segment
    
