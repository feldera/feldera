#!/bin/python3
import json
import os
import time
import requests
import argparse

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
EXAMPLE_SQL = os.path.join(SCRIPT_DIR, "program.sql")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    api_url = parser.parse_args().api_url

    # Test connectivity by fetching the existing pipelines
    print("Checking connectivity by listing pipelines...")
    response = requests.get(f"{api_url}/v0/pipelines?code=false")
    if response.ok:
        print("SUCCESS: can reach the API")
        print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
    else:
        print("FAILURE: could not reach API")
        print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
        exit(1)

    pipeline_name = "example"

    # Shut down the pipeline if it is not yet
    print("Shutting down the pipeline...")
    if requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").ok:
        requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["deployment_status"] != "Shutdown":
            time.sleep(1)

    # Create pipeline
    print("Creating pipeline...")
    program_sql = open(EXAMPLE_SQL).read()
    response = requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "name": pipeline_name,
        "description": "Description of the example pipeline",
        "runtime_config": {},
        "program_code": program_sql,
        "program_config": {}
    })
    if response.ok:
        print("SUCCESS: created pipeline")
        print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
    else:
        print("FAILURE: could not create pipeline")
        print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
        exit(1)

    # Wait for pipeline program compilation
    while True:
        response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
        if response.ok:
            pipeline = json.loads(response.content.decode("utf-8"))
            print("Program status: %s" % pipeline["program_status"])
            if pipeline["program_status"] == "Success":
                print("SUCCESS: pipeline program is compiled")
                break
            time.sleep(5)
        else:
            print("FAILURE: could not check pipeline")
            print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
            exit(1)

    # (Re)start the pipeline
    print("(Re)starting pipeline...")
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["deployment_status"] != "Shutdown":
        time.sleep(1)
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["deployment_status"] != "Running":
        time.sleep(1)
    print("Pipeline (re)started")

    # Continuously observe pipeline state
    while True:
        response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
        if response.ok:
            print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
            time.sleep(5)
        else:
            print("FAILURE: could not check pipeline")
            print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
            exit(1)


# Main entry point
if __name__ == "__main__":
    main()
