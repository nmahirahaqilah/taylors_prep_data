import pandas as pd
import numpy as np
import os
from urllib.parse import quote
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
from r2r_pipelines import export_db
import warnings
from config.constants import MAPPING_PATH
from r2r_pipelines import assign_intake_cycle, create_pg_connection
warnings.filterwarnings('ignore')

query_sf_opp_enr ="""
    select 
        reporting_date,
        programme_code::varchar as programme_code,
        opp_stage,
        withdrawn_pre_commencement,
        admission_status,
        acc_id,
        opp_id,
        prog_intake_month,
        prog_intake_year,
        programme1,
        prog_intake_year::varchar(256)||right('0'||prog_intake_month::varchar(256),2) as intake,
        intake_year,
        intake_month,
        "cycle",
        programme_status,
        programme_name,
        prev_intake_year,
        prev_intake_month,
        prev_prog_status,
        stage_prev_rec as prev_stage,
        programmename_prev_rec as prev_prog_name,
        ipt_note,
        intakeclosingdate,
        registered_date,
        bucket_domestic_int,
        owner_role,
        market_segment,
        micpa_caanzcount as micpa_caanz_count,
        micpa_caanzmodule as micpa_caanz_module,
        bursary_deduction as bursary_deduction_1,
        bursarydeduction_2 as bursary_deduction_2,
        scholarship_deduction,
        enrolledbyagent,
        agent,
        commissionamountforecast as commission_amount,
        state
    from sf_opp_enr   
    where registered_date <= '2099-12-31'
    """
    
def assign_segment_final(row):
    if row['bucket_domestic_int'] in ('International', 'ISR'):
        return 'International'
    elif row['owner_role'].startswith('ISR'):
        return 'International'
    elif row['market_segment'] == 'Progression':
        return 'Progression'
    else:
        return 'Domestic'
    
    
def base_enreg_filters(df):   
    return df[
        (df['withdrawn_pre_commencement'] == 'false') &
        (~df['admission_status'].isin(['Rejected (Assessment)',
                                       'Rejected (Entry Requirement)',
                                       'Offered - Decline)'])) &
        (~df['programme_status'].isin(['Withdrawn (Pre-commencement)',
                                       'Deferred (intake)',
                                       'Transfer Out']))
    ]


def base_ctd_filters(df):
    first_filter = base_enreg_filters(df)

    ctd_condition = first_filter[
        (first_filter['prog_cycle'] == first_filter['cycle']) &
        (first_filter['prog_intake_year'] == first_filter['intake_year']) &
        # Exclude students who transferred and already registered
        (
            (first_filter['prev_prog_status'].isnull()) |
            (~first_filter['prev_prog_status'].isin(['Transferred (Institution)', 
                                                     'Transfer Out', 
                                                     'Registered'])) |
            (
                # Handling for transferred students
                (first_filter['prev_prog_status'].isin([
                    'Transferred (Institution)', 
                    'Transfer Out'])) &
                # If their previous program name contains "intensive english", we include them
                (
                    (first_filter['prev_prog_name'].str.lower().str.contains('intensive english')) |
                    (
                        (first_filter['prev_stage'] != 'Registered') |
                        (
                            (first_filter['prev_stage'] == 'Registered') &
                            (first_filter['intake_year'] == first_filter['prev_intake_year']) &
                            (first_filter['cycle'] == first_filter['prev_cycle'])
                        )
                    )
                )
            )
        )
    ]

    return ctd_condition


def adjusted_programme_code(df, file_path = MAPPING_PATH, file_name = 'adj_map.xlsx'):
    # Merge the adjusted programme code to the main dataframe
    prog_code_adj = pd.read_excel(Path(file_path)/file_name, sheet_name='prog_code_correction', dtype=str)
    prog_code_adj = prog_code_adj.astype({
        'IntakeYear': 'int'
    })
    
    df = df.merge(prog_code_adj[['IntakeYear', 'ProgrammeCode', 'Revised Programme Code']], 
                  how='left', 
                  right_on=['IntakeYear', 'ProgrammeCode'], 
                  left_on=['intake_year', 'programme_code'])

    # Create a new column 'prog_code_adj' to store the adjusted programme code
    df['prog_code_adj'] = np.where(df['Revised Programme Code'].notnull(), 
                                   df['Revised Programme Code'], 
                                   df['programme_code'])

    # Drop the reference columns
    df.drop(columns=['Revised Programme Code', 'IntakeYear', 'ProgrammeCode'], inplace=True)
    return df


def adjusted_intake_month(adj_df, file_path = MAPPING_PATH, file_name = 'adj_map.xlsx'):
    special_sem_adj = pd.read_excel(Path(file_path)/file_name, sheet_name='special_sem', dtype=str)
    merge_cols = ['Intake Month Jarvis', 'ProgrammeCode', 'IntakeMonth TM1']

    v_df = adj_df.merge(special_sem_adj[merge_cols], 
                    how='left',
                    left_on=['intake', 'prog_code_adj'],
                    right_on=['Intake Month Jarvis', 'ProgrammeCode'])

    # Create "intake_adj" column to store the intake based on condition
    v_df['intake_adj'] = v_df['IntakeMonth TM1'].fillna(v_df['intake'])
    
    # Drop the reference column
    return v_df.drop(columns = merge_cols)


def extract_enreg_data():
    engine = create_pg_connection()

    with engine.connect() as connection:
        df = pd.read_sql_query(query_sf_opp_enr, connection)
    
    return base_enreg_filters(df)


def transform_enreg_data(df):
    df['prog_cycle'] = assign_intake_cycle(df, column_name='prog_intake_month')
    df['prev_cycle'] = assign_intake_cycle(df, column_name='prev_intake_month')
    df['market_segment'] = df.apply(assign_segment_final, axis=1)
    #df['is_enrolled_by_agent'] = df.apply(lambda row: True if row['enrolledbyagent'] == 'true' and len(str(row['agent'])) > 0 else False, axis=1)

    # adjust for prog_code mergers
    df = adjusted_programme_code(df)

    # adjust for special semester
    df = adjusted_intake_month(df)
    
    # Join with cycle calendar information
    cycle_calendar = extract_transform_cycle_calendar()
    df = df.merge(cycle_calendar[['prog_intake_year', 'cycle', 'cycle_end_date']], 
            on=['prog_intake_year', 'cycle'], how='left')

    # Final columns formatting
    df[['opp_id', 'acc_id']] = df[['opp_id', 'acc_id']].apply(lambda x: x.str[:15])

    date_columns = ['reporting_date', 'intakeclosingdate', 'registered_date','cycle_end_date']
    df[date_columns] = df[date_columns].apply(pd.to_datetime,format='%d/%m/%Y',errors='coerce')

    df['prev_intake_year'] = df['prev_intake_year'].fillna(0).astype(int)
    df = df.loc[df['withdrawn_pre_commencement'] == 'false'].copy()
    
    # !!! Consider removing this line if not needed (Manual adjustment)
    df = df[df['programme1']!='ACCA Qualification (IRW)'].reset_index(drop=True)
    
    return df

def extract_transform_acc_withdrawal(file_path = MAPPING_PATH, withdrawal_date = 'Closing_Withdrawal Date.xlsx', pg_acc_data = 'PG_Account_RawData_20250504.csv'):
    # CMS withdrawal data
    withdrawn = pd.read_excel(Path(file_path)/withdrawal_date, usecols=['Student #', 'Withdrawn Date', 'Course Code'])

    # PG Account Data
    acc_data = pd.read_csv(Path(file_path)/pg_acc_data, usecols=['Id', 'Student_Keys__c', 'LastActivityDate'])

    # in Id column in acc_data, take the first 15 characters
    acc_data['Id'] = acc_data['Id'].astype(str).str.slice(0, 15)

    # convert Student_Keys__c to int, if null then fill with 0, if error then fill with 0
    acc_data['student_keys'] = pd.to_numeric(acc_data['Student_Keys__c'], errors='coerce').astype('Int64')
    acc_data['last_activity_date'] = pd.to_datetime(acc_data['LastActivityDate'], errors='coerce')

    # Merge PG Account Data with Withdrawal Data
    acc_withdrawal = acc_data.merge(
        withdrawn.rename(columns={'Student #': 'student_id', 'Withdrawn Date': 'withdrawn_date', 'Course Code': 'course_code'}),
        left_on='student_keys',
        right_on='student_id',
        how='left'
    )
        
    return acc_withdrawal[['Id', 'student_keys', 'last_activity_date', 'student_id', 'withdrawn_date', 'course_code']]

def merge_acc_withdrawal(df):
    # Merge closing dataset with the account_withdrawal info
    acc_withdrawal = extract_transform_acc_withdrawal()
    
    # Filter for closing windows
    cls = df[df['reporting_date'] == df['cycle_end_date']]
    
    cls_withdrawal = cls.merge(acc_withdrawal[['Id', 'student_id', 'last_activity_date', 'withdrawn_date']], 
                               left_on='acc_id', right_on='Id', how='left')

    # Revert to the original index
    cls_withdrawal.set_index(cls.index, inplace=True)

    merged_df = df.merge(cls_withdrawal[['last_activity_date', 'withdrawn_date']], 
                         left_index=True, right_index=True, how='left')
    
    return merged_df

def extract_transform_cycle_calendar(file_path = MAPPING_PATH, file_name = "ImportDateStartNEndDate.xlsx"):
    # Academic Calendar -- To get the cycle end date and create the closing dataframe
    cycle_calendar = pd.read_excel(Path(file_path) / file_name, usecols=['IntakeYear', 'Cycle', 'EndDate']
                                   ).rename(columns={'IntakeYear': 'prog_intake_year', 
                                                     'Cycle': 'cycle',
                                                     'EndDate': 'cycle_end_date'})

    cycle_calendar['cycle_end_date'] = pd.to_datetime(cycle_calendar['cycle_end_date'], errors='coerce')
    return cycle_calendar

# CTD filters
def apply_enreg_filters(df):
    # Base filters        
    # CancelledRegistered Logic
    df['cancelled_registered'] = (df['programme_status'] == 'Cancelled').astype(int)

    # IPT Package Logic
    df['ipt_package'] = np.where(
        (df['prev_prog_status'].isin(['Transfer Out'])) &
        (df['programme_name'].str.contains('Pharmacy', case=False, na=False)) &
        (df['prev_prog_name'].str.contains('Biotechnology', case=False, na=False)),
        1, 0   
    )

    # IPT Same Year Logic
    df['ipt_same_year'] = np.where(
        (df['prev_intake_year'] == df['prog_intake_year']) &
        (df.apply(lambda row: row['prev_intake_month'] in 
            (set() if row['cycle_end_date'].month < 5 
                else {1, 2} if row['cycle_end_date'].month < 9 
                else {1, 2, 3, 4, 5, 6}), axis=1)) &
        (df['prev_prog_status'].isin(['Transfer Out', 'Transferred (Institution)', 'Registered'])),
        1, 0
    )

    # IPT Without Task Logic
    df['ipt_without_task'] = np.where(
        df['prev_prog_status'].eq('Transfer Out') 
        & df['ipt_note'].fillna("").str.strip().eq("IPT without task"),
        1, 0
    )
    
    # Withdrawal_PreComm_Flag Logic (Matched)
    def calculate_withdrawal_precomm(row):
        if pd.isna(row['programme_status']):  # Ignore if programme_status is None or NaN
            return 0
        
        if 'Withdrawn' in row['programme_status']:
            if pd.isna(row['withdrawn_date']) or (
                row['withdrawn_date'] > row['intakeclosingdate'] and row['withdrawn_date'] > row['registered_date']
            ):
                return 0
            else:
                return 1
        return 0

    # IPT Previous Year Logic (Matched)
    def calculate_ipt_prev_year(row):
        if (row['prev_intake_year'] < row['prog_intake_year']
            ) and row['prev_prog_status'] == 'Transfer Out' and (
            isinstance(row['ipt_note'], str) and 'previous year' in row['ipt_note'].lower()):
            return 1
        elif row['ipt_note'] == 'IPT without task' and row['prev_prog_status'] == 'Transfer Out':
            return 0
        else:
            return np.nan
        
    # Withdrawal Pre-Comm + IPT Previous Year Logics
    df['withdrawal_pre_comm'] = df.apply(calculate_withdrawal_precomm, axis=1)
    df['ipt_prev_year'] = df.apply(calculate_ipt_prev_year, axis=1)
    
    # if the sum of cancelled_registered, ipt_package, ipt_same_year, ipt_without_task, withdrawal_pre_comm is more than 0, then final =1, else 0
    df['enreg_count'] = np.where(
        df[['cancelled_registered', 'ipt_package', 'ipt_same_year', 'ipt_without_task', 'withdrawal_pre_comm']]\
        .sum(axis=1) > 0, 0, 1
    )
    
    return df

def preprocess_ctd_enreg():
    main_df = extract_enreg_data()
    main_df.reset_index(drop=True, inplace=True)

    
    processed_df =  (main_df.copy().pipe(transform_enreg_data).pipe(merge_acc_withdrawal).pipe(apply_enreg_filters))

    date_columns = ['reporting_date','registered_date','cycle_end_date']

    for col in date_columns:
        processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce').dt.strftime('%d/%m/%Y')
        processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')

    engine = export_db.marcommdb_connection()
    processed_df.to_sql('ctd_enreg', engine, schema='public', if_exists='replace', index=False)

    return processed_df