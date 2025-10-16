import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from r2r_pipelines import prep_daily_ctd_enreg

prep_daily_ctd_enreg.fetch_and_store_sf_opportunities()