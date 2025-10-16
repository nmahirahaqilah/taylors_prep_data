import os

STG_DIR = "//10.99.75.198/Qlik"
PROD_DIR = "//10.99.64.144/Qlik"

DNA_SANDBOX_PATH = os.path.join(STG_DIR, "dna_sandbox")
RAW_DATA_PATH = os.path.join(DNA_SANDBOX_PATH, "raw_data")
CLEAN_DATA_PATH = os.path.join(DNA_SANDBOX_PATH, "clean_data")

# Mapping raw data paths
MAPPING_PATH = os.path.join(RAW_DATA_PATH, "mapping_files")

# cycle closing raw data paths
CYCLE_CLOSING_PATH = os.path.join(RAW_DATA_PATH, "cycle_closing")

# finance fee data paths
FINANCE_FEE_PATH = os.path.join(RAW_DATA_PATH, "finance_fee")

# Historical Annual Target
ANNUAL_TARGET_PATH = os.path.join(RAW_DATA_PATH, "annual_target")

# Cycle Preplanning raw data paths
CPP_DATA_PATH = os.path.join(RAW_DATA_PATH, "cycle_preplanning")
CPP_ENREG_PATH = os.path.join(CPP_DATA_PATH, "cpp_enreg")
CPP_NR_PATH = os.path.join(CPP_DATA_PATH, "cpp_nr")

# External market raw data paths
RM_MOHE_PATH = os.path.join(RAW_DATA_PATH, "mohe_database")
PRICING_MOHE_PATH = os.path.join(RAW_DATA_PATH, "pricing_dataset")

# TM1 raw data paths
TM1_ANNUAL_PATH = os.path.join(RAW_DATA_PATH, "tm1_annual_data")

# File Extensions
EXCEL_FILE_EXTENSION = ".xlsx"
