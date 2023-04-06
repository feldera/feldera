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

    rpk['topic', 'delete', 'fraud_demo_large_demographics']()
    rpk['topic', 'delete', 'fraud_demo_large_transactions']()
    rpk['topic', 'delete', 'fraud_demo_large_enriched']()
    rpk['topic', 'create', 'fraud_demo_large_demographics',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'fraud_demo_large_transactions',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'fraud_demo_large_enriched']()

    transactions_csv = os.path.join(
        SCRIPT_DIR, 'transactions.csv')
    demographics_csv = os.path.join(
        SCRIPT_DIR, 'demographics.csv')

    if not os.path.exists(transactions_csv):
        from plumbum.cmd import gdown
        print("Downloading transactions.csv (~2 GiB)...")
        gdown['1RBEDUuvb-L15dk_UE9PPv3PgVPmkXJy6',
              '--output', transactions_csv]()

    # Push test data to topics
    print('Pushing demographics data to Kafka topic...')
    with open(demographics_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce', 'fraud_demo_large_demographics',
             '-f', '%v'] << '\n'.join(n_lines))()
    print('Pushing transaction data to Kafka topic...')
    with open(transactions_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 8_000)), ()):
            (rpk['topic', 'produce',
                 'fraud_demo_large_transactions', '-f', '%v'] << '\n'.join(n_lines))()


def make_config(project):
    config = DBSPPipelineConfig(project, 8, 'TimeSeriesEnrich Pipeline')
    config.add_kafka_input(name='Fraud Demographics Large', stream='DEMOGRAPHICS',
                           config=KafkaInputConfig.from_dict(
                               {'topics': ['fraud_demo_large_demographics'], 'auto.offset.reset': 'earliest'}), format=CsvInputFormatConfig())
    config.add_kafka_input(name='Fraud Transactions Large', stream='TRANSACTIONS',
                           config=KafkaInputConfig.from_dict(
                               {'topics': ['fraud_demo_large_transactions'], 'auto.offset.reset': 'earliest'}), format=CsvInputFormatConfig())
    config.add_kafka_output(name='Join Demographics<>Transactions', stream='TRANSACTIONS_WITH_DEMOGRAPHICS',
                            config=KafkaOutputConfig.from_dict(
                                {'topic': 'fraud_demo_large_enriched'}), format=CsvOutputFormatConfig())
    config.save()
    return config


if __name__ == "__main__":
    run_demo("Time Series Enrichment Join", os.path.join(
        SCRIPT_DIR, 'project.sql'), make_config, prepare)
