# This code is borrowed from the Hopsworks TikTok RecSys Demo

# Original source: https://github.com/davitbzh/tiktok-recsys/blob/main/python/Jupyter/streaming/config.py

USERS_AMOUNT_HISTORICAL = 1_000
VIDEO_AMOUNT_HISTORICAL = 1_000
INTERACTIONS_AMOUNT_HISTORICAL = 50_000_000

USERS_AMOUNT_PIPELINE = 1_000
VIDEO_AMOUNT_PIPELINE = 1_000
INTERACTIONS_AMOUNT_PIPELINE = 1_000_000

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DAY_FORMAT = '%Y-%m-%d'
MONTH_FORMAT = '%Y-%m-01 00:00:00'

KAFKA_TOPIC_NAME = "interactions_streaming_test_trial2"
SCHEMA_NAME = "interactions_streaming_test_trial_schema1"

KAFKA_SERVER = "localhost:19092"
#KAFKA_SERVER_FROM_PIPELINE = "redpanda:9092"
KAFKA_SERVER_FROM_PIPELINE = "localhost:19092"
