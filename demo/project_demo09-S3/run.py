# Read all objects with the prefix "foo" from an S3 bucket. The S3 bucket is pre-loaded with two objects,
# foo_1.csv and foo_2.csv, that have values (1, 2, 3) and (4, 5, 6).
#
# It expects two environment varaibles CI_S3_AWS_ACCESS_KEY and CI_S3_AWS_SECRET for the test IAM user permissions.
#
# To run the demo, start Feldera:
# > docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-dev.yml --profile s3-demo \
#       up db pipeline-manager --build --renew-anon-volumes --force-recreate
#
# Run this script:
# > python3 run.py --api-url=http://localhost:8080 --start
import base64
import os
import time
import datetime
import requests
import argparse
from plumbum.cmd import rpk
import psycopg
import json
import random

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROGRAM_SQL = os.path.join(SCRIPT_DIR, "program.sql")

DATABASE_NAME = "jdbc_test_db"
PIPELINE_NAME = "demo-s3"
FELDERA_CONNECTOR_NAME = "s3-input"
TABLE_NAME = "test_table"
VIEW_NAME = "test_view"
PRIMARY_KEY_COLUMN = "id"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument('--start', action='store_true', default=False, help="Start the Feldera pipeline")
    args = parser.parse_args()
    prepare_feldera_pipeline(args.api_url, args.start)

def prepare_feldera_pipeline(api_url, start_pipeline):
    s3_access_key = os.getenv("CI_S3_AWS_ACCESS_KEY")
    s3_secret_key = os.getenv("CI_S3_AWS_SECRET")
    assert s3_access_key is not None and s3_secret_key is not None

    # Create program
    program_name = "demo-s3-input"
    program_sql = open(PROGRAM_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "S3 input connector demo",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {program_name} (version: {program_version})...")
    requests.post(f"{api_url}/v0/programs/{program_name}/compile", json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Connectors
    connectors = []
    requests.put(f"{api_url}/v0/connectors/{FELDERA_CONNECTOR_NAME}", json={
        "description": "",
        "config": {
            "format": {
                "name": "csv",
            },
            "transport": {
                "name": "s3",
                "config": {
                    "credentials": {
                        "type": "AccessKey",
                        "aws_access_key_id": s3_access_key,
                        "aws_secret_access_key": s3_secret_key,
                    },
                    "bucket_name": "feldera-connector-test-bucket",
                    "region": "us-west-1",
                    "read_strategy": {
                        "type": "Prefix",
                        "prefix": "foo",
                    }
                }
            }
        }
    })
    connectors.append({
        "connector_name": FELDERA_CONNECTOR_NAME,
        "is_input": True,
        "name": FELDERA_CONNECTOR_NAME,
        "relation_name": TABLE_NAME
    })

    # Create pipeline
    requests.put(f"{api_url}/v0/pipelines/{PIPELINE_NAME}", json={
        "description": "",
        "config": {
            "workers": 1,
        },
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()

    # Start pipeline
    if start_pipeline:
        print("(Re)starting pipeline...")
        requests.post(f"{api_url}/v0/pipelines/{PIPELINE_NAME}/shutdown").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{PIPELINE_NAME}").json()["state"]["current_status"] != "Shutdown":
            time.sleep(1)
        requests.post(f"{api_url}/v0/pipelines/{PIPELINE_NAME}/start").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{PIPELINE_NAME}").json()["state"]["current_status"] != "Running":
            time.sleep(1)
        print("Pipeline (re)started")

        # Wait and then check the output view to see if our results are ready
        time.sleep(2)
        response = requests.post(f"{api_url}/v0/pipelines/{PIPELINE_NAME}/egress/{VIEW_NAME}?format=csv&query=quantiles&mode=snapshot")
        assert response.json()['text_data'] == '1,1\n2,1\n3,1\n4,1\n5,1\n6,1\n'

if __name__ == "__main__":
    main()
