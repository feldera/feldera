# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "feldera @ file:///${PROJECT_ROOT}/python",
#     "requests<3",
# ]
# ///

#!/bin/python3

# Run locally with:
#   uv run demo/all-packaged/run.py --api-url http://localhost:8080

import time
import requests
import argparse
from feldera import FelderaClient, PipelineBuilder
from feldera.enums import PipelineStatus


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080)"
    )
    args = parser.parse_args()

    # Create Feldera client using the API URL
    api_url = args.api_url
    print(f"Feldera API URL: {api_url}")
    client = FelderaClient(api_url)

    # Retrieve and run all packaged demos
    demos = requests.get(f"{api_url}/v0/config/demos").json()
    print(f"Total {len(demos)} packages demos were found and will be run")
    for demo in demos:
        print(f"Running packaged demo: {demo['name']}...")
        pipeline = PipelineBuilder(
            client,
            demo["name"],
            demo["program_code"],
            udf_rust=demo["udf_rust"],
            udf_toml=demo["udf_toml"],
        ).create_or_replace()
        pipeline.start()
        time.sleep(2)
        status = pipeline.status()
        assert status == PipelineStatus.RUNNING, f"FAIL: demo {
            demo['name']
        }: expected pipeline to be RUNNING but instead is {status}"
        pipeline.stop(force=True)
        print(f"PASS: demo {demo['name']}")


# Main entry point
if __name__ == "__main__":
    main()
