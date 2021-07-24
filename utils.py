import configparser, sys, os
from pathlib import Path

# Check for environment varialb "PRA_HOME"
if "PRA_HOME" not in os.environ:
    print("Please set environment variable PRA_HOME before running.")
    sys.exit(1)

PRA_HOME = os.environ['PRA_HOME']
sys.path.insert(1, PRA_HOME)

config = configparser.ConfigParser()
config.read(Path(PRA_HOME).joinpath("config.cfg"))

CONNECTION_OBJECT = config._sections["DATABASE"]
AIRFLOW_CONFIG = config._sections["AIRFLOW"]