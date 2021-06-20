import configparser, sys, os
from pathlib import Path

# Check for environment varialb "PRA_HOME"
if "PRA_HOME" not in os.environ:
    print("Please set environment variable PRA_HOME before running.")
    sys.exit(1)

PROJECT_PATH = os.environ['PRA_HOME']

config = configparser.ConfigParser()
config.read(Path(PROJECT_PATH).joinpath("config.cfg"))

CONNECTION_OBJECT = config._sections["DATABASE"]