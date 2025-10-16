import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from r2r_pipelines import prep_annual_targets

prep_annual_targets.preprocess_annual_targets()

print("Done")

