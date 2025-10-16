import pandas as pd
import numpy as np
import os
from urllib.parse import quote
from sqlalchemy import create_engine
from dotenv import load_dotenv

def create_pg_connection(user_name = "PG_USERNAME",
                             pass_word = "PG_PASSWORD",
                             host = "PG_HOST",
                             port = "PG_PORT",
                             database = "PG_DATABASE"):
    load_dotenv()
    password = os.getenv(pass_word)
    encoded_password = quote(password)

    username = os.getenv(user_name)
    host = os.getenv(host)
    port = os.getenv(port)
    database = os.getenv(database)

    DATABASE_URL = f"postgresql+psycopg2://{username}:{encoded_password}@{host}:{port}/{database}"
    
    return create_engine(DATABASE_URL)

def assign_intake_cycle(df, column_name='prog_intake_month'):
    """
    Assigns 'C1', 'C2', 'C3' or NA based on the intake month using if-else statements.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column_name (str): Column name containing intake months.

    Returns:
    pd.Series: A new column with assigned cycle values.
    """
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    
    def get_cycle(month):
        if pd.isna(month):
            return pd.NA
        elif month < 3:
            return 'C1'
        elif month < 7:
            return 'C2'
        elif month < 13:
            return 'C3'
        else:
            return pd.NA

    return df[column_name].apply(get_cycle)

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
                                       'Transfer Out'])) # include cancelled, cancel-registered
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

query_sf_opp_enr ="""
    select 
        reporting_date,
        registered_date
        programme_code,
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
        bucket_domestic_int,
        owner_role,
        market_segment
    from sf_opp_enr   
    where registered_date <= '2099-12-31'
    """

def preprocess_enreg_data():
    engine = create_pg_connection()

    with engine.connect() as connection:
        df = pd.read_sql_query(query_sf_opp_enr, connection)
    print("Data loaded successfully from csm_sas database")
    
    df['prog_cycle'] = assign_intake_cycle(df, column_name='prog_intake_month')
    df['prev_cycle'] = assign_intake_cycle(df, column_name='prev_intake_month')
    df['segment_final'] = df.apply(assign_segment_final, axis=1)
    print("Data preprocessed successfully")

    return base_enreg_filters(df)