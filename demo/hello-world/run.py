from itertools import islice
import sys
import os

from dbsp import DBSPPipelineConfig
from dbsp import CsvInputFormatConfig, CsvOutputFormatConfig

# Import
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))
from demo import *

SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
RECORDS_CSV = os.path.join(SCRIPT_DIR, 'records.csv')
MESSAGES_CSV = os.path.join(SCRIPT_DIR, 'messages.csv')


def prepare():
    if not os.path.exists(MESSAGES_CSV):
        raise Exception('Input CSV file {} not found', MESSAGES_CSV)
    if not os.path.exists(RECORDS_CSV):
        raise Exception('Input CSV file {} not found', RECORDS_CSV)


def make_config(project):
    config = DBSPPipelineConfig(project, 6, 'Hello World Pipeline')
    config.add_file_input(
        stream='MESSAGES', filepath=MESSAGES_CSV, format=CsvInputFormatConfig()),
    config.add_file_input(
        stream='RECORDS', filepath=RECORDS_CSV, format=CsvOutputFormatConfig()),
    config.save()
    return config


def verify():
    with open(RECORDS_CSV) as f:
        content = f.read()
        assert content == '8,"x"\n4,"world!"\n'


if __name__ == "__main__":
    run_demo("Hello World SQL", os.path.join(
        SCRIPT_DIR, 'combiner.sql'), make_config, prepare, verify)
