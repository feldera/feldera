from itertools import islice
import sys
import os

from dbsp import DBSPPipelineConfig
from dbsp import CsvInputFormatConfig, CsvOutputFormatConfig
from dbsp import KafkaInputConfig
from dbsp import KafkaOutputConfig

# Import
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))
from demo import *

SCRIPT_DIR = os.path.join(os.path.dirname(__file__))


def prepare():
    "Prepare Kafka topics for the demo."
    from plumbum.cmd import rpk

    rpk['topic', 'delete', 'null_demo_input']()
    rpk['topic', 'delete', 'null_demo_output']()
    rpk['topic', 'create', 'null_demo_input',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'null_demo_output']()

    data_csv = os.path.join(SCRIPT_DIR, 'data.csv')

    # Push test data to topics
    print('Pushing data to Kafka topic...')
    with open(data_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce', 'fraud_demo_large_demographics',
             '-f', '%v'] << '\n'.join(n_lines))()


def make_config(project):
    config = DBSPPipelineConfig(project, 6, 'SimpleSelect Pipeline')
    config.add_kafka_input(name='DEMOGRAPHICS', stream='USERS',
                           config=KafkaInputConfig.from_dict(
                               {'topics': ['null_demo_input'], 'auto.offset.reset': 'earliest'}), format=CsvInputFormatConfig())
    config.add_kafka_output(name='TRANSACTIONS', stream='OUTPUT_USERS',
                            config=KafkaOutputConfig.from_dict(
                                {'topic': 'null_demo_output'}), format=CsvOutputFormatConfig())
    config.save()
    return config


if __name__ == "__main__":
    run_demo("Simple Data Selection", os.path.join(
        SCRIPT_DIR, 'project.sql'), make_config, prepare)
