#!/bin/python3

import os
import time
import requests
import datetime
import argparse

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
EXAMPLE_SQL = os.path.join(SCRIPT_DIR, "program.sql")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True,
                        help="Feldera REST API URL (e.g., http://localhost:8080 or https://sandbox-staging.feldera.com")
    parser.add_argument('--num-total', required=False, default=10000000000000000000000000)
    parser.add_argument('--target-rate-per-second', required=True)
    parser.add_argument("--with-bearer-token", required=False,
                        help="Authorization Bearer token")
    args = parser.parse_args()
    if args.with_bearer_token is None:
        headers = {}
    else:
        headers = {
            "authorization": f"Bearer {args.with_bearer_token}"
        }

    # Feldera REST API URL
    api_url = args.api_url
    print(f"Feldera REST API URL: {api_url}")

    # Total
    num_total = int(args.num_total)
    print(f"Total number: {num_total}")

    # Target rate
    target_rate_per_second = int(args.target_rate_per_second)
    print(f"Target rate per second: {target_rate_per_second}")

    # Test connectivity by fetching existing programs
    requests.get(f"{api_url}/v0/programs", headers=headers).raise_for_status()

    # Create program
    program_name = "demo-steady-program"
    program_sql = open(EXAMPLE_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", headers=headers, json={
        "description": "",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {program_name} (version: {program_version})...")
    requests.post(f"{api_url}/v0/programs/{program_name}/compile", headers=headers, json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}", headers=headers).json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Create pipeline
    pipeline_name = "demo-steady-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers, json={
        "description": "",
        "config": {"workers": 8},
        "program_name": program_name,
        "connectors": [],
    }).raise_for_status()

    # Start pipeline
    print("(Re)starting pipeline...")
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown", headers=headers).raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers).json()["state"]["current_status"] != "Shutdown":
        time.sleep(1)
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start", headers=headers).raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers).json()["state"]["current_status"] != "Running":
        time.sleep(1)
    print("Pipeline (re)started")

    # Produce rows into the table
    print(f"Inserting rows into table example...")
    start_time_item_entry_generation = datetime.datetime.now().timestamp()
    for entry_no in range(1, num_total + 1):
        # Rate limiting
        elapsed_s = datetime.datetime.now().timestamp() - start_time_item_entry_generation
        target_generated = target_rate_per_second * elapsed_s
        if float(entry_no - 1) > target_generated:
            delta_too_much = float(entry_no - 1) - target_generated
            sleep_time_s = delta_too_much / target_rate_per_second
            print(f"Total elapsed time: {elapsed_s}")
            print(f"Generated so far: {entry_no - 1}")
            time.sleep(sleep_time_s)
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/ingress/EXAMPLE?format=json&array=false",
            json={"insert": {"id": entry_no}}, headers=headers
        ).raise_for_status()
    print("Finished")


# Main entry point
if __name__ == "__main__":
    main()