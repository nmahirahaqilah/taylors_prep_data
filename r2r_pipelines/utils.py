import pandas as pd
import os
from urllib.parse import quote
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config.constants import MAPPING_PATH
from pathlib import Path

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


def extract_ict_calendar(file_path = MAPPING_PATH, acad_calendar_file = "ImportDateStartNEndDate.xlsx"):
    # Academic Calendar -- To get the cycle end date and create the closing dataframe
    acad_calendar = pd.read_excel(Path(file_path)/acad_calendar_file)

    # Academic Calendar -- To get the cycle end date and create the closing dataframe
    acad_calendar.rename(columns={'IntakeYear': 'prog_intake_year', 
                                  'Cycle': 'cycle',
                                  'StartDate': 'cycle_start_date',
                                  'EndDate': 'cycle_end_date'}, inplace=True)

    acad_calendar['cycle_start_date'] = pd.to_datetime(acad_calendar['cycle_start_date'])
    acad_calendar['cycle_end_date'] = pd.to_datetime(acad_calendar['cycle_end_date'])
    
    return acad_calendar


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


def extract_prog_master(file_path = MAPPING_PATH, file_name = "prog_master_file.xlsx"):
    # read programme master file mapping
    return pd.read_excel(Path(file_path)/file_name, sheet_name="prog_master_code")

    