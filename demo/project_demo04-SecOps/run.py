from itertools import islice
import os
import sys
import subprocess

from dbsp import DBSPPipelineConfig
from dbsp import CsvInputFormatConfig
from dbsp import KafkaInputConfig

# Import
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".."))
from demo import *

SCRIPT_DIR = os.path.join(os.path.dirname(__file__))


def prepare():
    cmd = ["cargo", "run", "--release"]
    # Override --release if RUST_BUILD_PROFILE is set
    if "RUST_BUILD_PROFILE" in os.environ:
        cmd[-1] = os.environ["RUST_BUILD_PROFILE"]
    subprocess.run(cmd, cwd=os.path.join(SCRIPT_DIR, "simulator"))


def make_config(project):
    config = DBSPPipelineConfig(project, 8, "SecOps Pipeline")

    config.add_kafka_input(
        name="secops_pipeline_sources",
        stream="PIPELINE_SOURCES",
        config=KafkaInputConfig.from_dict(
            {"topics": ["secops_pipeline_sources"], "auto.offset.reset": "earliest"}
        ),
        format=CsvInputFormatConfig(),
    )
    config.add_kafka_input(
        name="secops_artifact",
        stream="ARTIFACT",
        config=KafkaInputConfig.from_dict(
            {"topics": ["secops_artifact"], "auto.offset.reset": "earliest"}
        ),
        format=CsvInputFormatConfig(),
    )
    config.add_kafka_input(
        name="secops_vulnerability",
        stream="VULNERABILITY",
        config=KafkaInputConfig.from_dict(
            {"topics": ["secops_vulnerability"], "auto.offset.reset": "earliest"}
        ),
        format=CsvInputFormatConfig(),
    )
    config.add_kafka_input(
        name="secops_cluster",
        stream="K8SCLUSTER",
        config=KafkaInputConfig.from_dict(
            {"topics": ["secops_cluster"], "auto.offset.reset": "earliest"}
        ),
        format=CsvInputFormatConfig(),
    )
    config.add_kafka_input(
        name="secops_k8sobject",
        stream="K8SOBJECT",
        config=KafkaInputConfig.from_dict(
            {"topics": ["secops_k8sobject"], "auto.offset.reset": "earliest"}
        ),
        format=CsvInputFormatConfig(),
    )
    config.add_file_output(
        stream="K8SCLUSTER_VULNERABILITY_STATS",
        filepath=os.path.join(SCRIPT_DIR, "k8scluster_vulnerability_stats.csv"),
        format=CsvInputFormatConfig(),
    )

    config.save()
    return config


if __name__ == "__main__":
    run_demo(
        "SecOps demo", os.path.join(SCRIPT_DIR, "project.sql"), make_config, prepare
    )
