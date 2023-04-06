from itertools import islice
import os
import sys

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

    rpk['topic', 'delete', 'green_trip_demo_large_input']()
    rpk['topic', 'delete', 'green_trip_demo_large_output']()
    rpk['topic', 'create', 'green_trip_demo_large_input',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'green_trip_demo_large_output']()

    green_tripdata_csv = os.path.join(
        SCRIPT_DIR, 'green_tripdata.csv')

    if not os.path.exists(green_tripdata_csv):
        print("Downloading green_tripdata.csv...")
        from plumbum.cmd import gdown
        gdown['14cKfJjwhsVPosshmSP7MBrolsTJfz9Xh',
              '--output', green_tripdata_csv]()

    # Push test data to topics
    print('Pushing tripdata data to Kafka topic...')
    with open(green_tripdata_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 10_000)), ()):
            (rpk['topic', 'produce', 'green_trip_demo_large_input',
             '-f', '%v'] << '\n'.join(n_lines))()


def make_config(project):
    config = DBSPPipelineConfig(
        project, 8, 'GreenTrip Pipeline')
    config.add_kafka_input(name='Green Trips Data',
                           stream='GREEN_TRIPDATA',
                           config=KafkaInputConfig.from_dict(
                               {'topics': ['green_trip_demo_large_input'], 'auto.offset.reset': 'earliest'}), format=CsvInputFormatConfig())
    config.add_kafka_output(name='Online Feature Calculation',
                            stream='FEATURES',
                            config=KafkaOutputConfig.from_dict(
                                {'topic': 'green_trip_demo_large_output'}), format=CsvOutputFormatConfig())
    config.save()
    return config


if __name__ == "__main__":
    run_demo("GreenTrip Feature Streaming", os.path.join(
        SCRIPT_DIR, 'project.sql'), make_config, prepare)
