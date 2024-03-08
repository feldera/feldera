#!/bin/python3

# This script is used to measure Rust compilation speed.
# It takes a SQL program and first measures the time it takes to compile the whole
# program from scratch.  It then splits the SQL code into individual
# queries and adds them to the program one by one, measuring
# the time it takes to re-compile the program.

# NOTE: The script doesn't do proper SQL parsing, but simply splits the program
# into semicolon-delimited chunks.  Make sure there are no semicolons in
# comments or inside queries.


import os
import time
import requests
import argparse
import datetime
import json
import joblib
import multiprocessing
from joblib import Parallel, delayed
from typing import List
from typing import Callable
from faker import Faker
from faker.providers import company
from faker.providers import DynamicProvider
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))

api_url = ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-program", required=True,
                        help="SQL program to compile")
    parser.add_argument("--api-url", required=True,
                        help="Feldera REST API URL (e.g., http://localhost:8080 or https://sandbox-staging.feldera.com")
    parser.add_argument("--with-bearer-token", required=False,
                        help="Authorization Bearer token")
    args = parser.parse_args()

    # Feldera REST API URL
    global api_url
    api_url = args.api_url
    print(f"Feldera REST API URL: {api_url}")
    print(f"SQL program: {args.sql_program}")

    if args.with_bearer_token is None:
        headers = {}
    else:
        headers = {
            "authorization": f"Bearer {args.with_bearer_token}"
        }

    # Test connectivity by fetching existing programs
    requests.get(f"{api_url}/v0/programs", headers=headers).raise_for_status()

    # Create program
    program_name = "compilation_speed_benchmark"

    program_sql = open(args.sql_program).read()
    queries = program_sql.split(';')
    print(f"Found {len(queries)} SQL fragments")

    response = requests.delete(f"{api_url}/v0/programs/{program_name}", headers=headers)

    print(f"Compiling full program")
    compile(program_name, program_sql, headers)

    # Add queries one-by-one
    program = ""
    times = []
    for i in range(0, len(queries)):
        print(f"Compiling program with {i+1} queries")
        program += queries[i]
        program += ";"
        # print(f"program: {program}")
        elapsed = compile(program_name, program, headers)
        times.append((i+1, elapsed))

    for row in times:
        print(f"{row[0]}, {row[1]}")

def compile(program_name, program, headers):
    response = requests.put(f"{api_url}/v0/programs/{program_name}", headers=headers, json={
        "description": "",
            "code": program
    })

    response.raise_for_status()
    program_version = response.json()["version"]

    start = time.time()
    requests.post(f"{api_url}/v0/programs/{program_name}/compile", headers=headers, json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}", headers=headers).json()["status"]
        # print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(1)

    elapsed = time.time() - start
    print(f"Finished in {elapsed}s")
    return elapsed

# Main entry point
if __name__ == "__main__":
    main()
