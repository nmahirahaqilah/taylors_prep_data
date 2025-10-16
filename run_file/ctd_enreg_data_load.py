import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from r2r_pipelines import prep_ctd_enreg

prep_ctd_enreg.preprocess_ctd_enreg()

print("Done")

