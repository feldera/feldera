# This code is borrowed from the Hopsworks TikTok RecSys Demo

# Original source: https://github.com/davitbzh/tiktok-recsys/blob/main/python/Jupyter/streaming/config.py
import os

USERS_AMOUNT_HISTORICAL = 1_000
VIDEO_AMOUNT_HISTORICAL = 1_000
INTERACTIONS_AMOUNT_HISTORICAL = 50_000_000

USERS_AMOUNT_PIPELINE = 1_000
VIDEO_AMOUNT_PIPELINE = 1_000
INTERACTIONS_AMOUNT_PIPELINE = 1_000_000

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DAY_FORMAT = "%Y-%m-%d"
MONTH_FORMAT = "%Y-%m-01 00:00:00"

KAFKA_TOPIC_NAME = os.environ.get("KAFKA_TOPIC") or "interactions"

# default for running feldera and redpanda with docker
KAFKA_SERVER = os.environ.get("KAFKA_SERVER") or "localhost:19092"
KAFKA_SERVER_FROM_PIPELINE = (
    os.environ.get("KAFKA_SERVER_FROM_PIPELINE") or "redpanda:9092"
)

DATA_FMT = "csv" if os.environ.get("DATA_FMT") == "csv" else "json"
