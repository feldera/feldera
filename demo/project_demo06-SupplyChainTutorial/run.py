from itertools import islice
import os
import sys
import subprocess
from shutil import which

from dbsp import DBSPPipelineConfig
from dbsp import JsonInputFormatConfig, JsonOutputFormatConfig
from dbsp import KafkaInputConfig
from dbsp import KafkaOutputConfig
from dbsp import UrlInputConfig

# Import
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".."))
from demo import *

SCRIPT_DIR = os.path.join(os.path.dirname(__file__))


def prepare(args=[]):
    from plumbum.cmd import rpk

    rpk["topic", "delete", "price"]()
    rpk["topic", "create", "price"]()
    rpk["topic", "delete", "preferred_vendor"]()
    rpk["topic", "create", "preferred_vendor"]()


def make_config(project):
    config = DBSPPipelineConfig(
        project,
        8,
        "Feldera Basics Tutorial Pipeline",
        "See https://www.feldera.com/docs/tutorials/basics/",
    )

    config.add_url_input(
        name="tutorial-part-s3",
        stream="PART",
        config=UrlInputConfig.from_dict(
            {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"}
        ),
        format=JsonInputFormatConfig(update_format="insert_delete"),
    )
    config.add_url_input(
        name="tutorial-vendor-s3",
        stream="VENDOR",
        config=UrlInputConfig.from_dict(
            {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json"}
        ),
        format=JsonInputFormatConfig(update_format="insert_delete"),
    )
    config.add_url_input(
        name="tutorial-price-s3",
        stream="PRICE",
        config=UrlInputConfig.from_dict(
            {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"}
        ),
        format=JsonInputFormatConfig(update_format="insert_delete"),
    )

    config.add_kafka_input(
        name="tutorial-price-redpanda",
        stream="PRICE",
        config=KafkaInputConfig.from_dict(
            {
                "topics": ["price"],
                "group.id": "tutorial-price",
                "auto.offset.reset": "earliest",
            }
        ),
        format=JsonInputFormatConfig(update_format="insert_delete"),
    )
    config.add_kafka_output(
        name="tutorial-preferred_vendor-redpanda",
        stream="PREFERRED_VENDOR",
        config=KafkaOutputConfig.from_dict({"topic": "preferred_vendor"}),
        format=JsonOutputFormatConfig(),
    )

    config.save()
    return config


if __name__ == "__main__":
    run_demo(
        "Feldera Basics Tutorial",
        os.path.join(SCRIPT_DIR, "project.sql"),
        make_config,
        prepare,
    )
