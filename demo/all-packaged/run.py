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
from feldera.testutils import _get_effective_api_key
from feldera.testutils_oidc import setup_token_cache


def main():
    # Initialize OIDC token cache before any API calls (reuses pytest logic)
    setup_token_cache()

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080)"
    )
    args = parser.parse_args()

    # Create Feldera client using the API URL with OIDC token caching support
    api_url = args.api_url
    print(f"Feldera API URL: {api_url}")

    # Get effective API key (OIDC token takes precedence over static API key)
    # This reuses the same token caching infrastructure as the Python test suite
    effective_api_key = _get_effective_api_key()
    if effective_api_key:
        print("🔐 AUTH: Using cached OIDC authentication")
        client = FelderaClient(api_url, api_key=effective_api_key)
    else:
        print("🔐 AUTH: Using no authentication")
        client = FelderaClient(api_url)

    # Retrieve and run all packaged demos
    # Use the same authentication headers for the demos request
    headers = {"Accept": "application/json"}
    if effective_api_key:
        headers["Authorization"] = f"Bearer {effective_api_key}"

    demos = requests.get(f"{api_url}/v0/config/demos", headers=headers).json()
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
