import os
import time
import requests
import argparse

# File locations
DEMO_DIR = os.path.join(os.path.dirname(__file__))
RECORDS_CSV = os.path.join(DEMO_DIR, "records.csv")
MESSAGES_CSV = os.path.join(DEMO_DIR, "messages.csv")
COMBINER_SQL = os.path.join(DEMO_DIR, "combiner.sql")
MATCHES_CSV = os.path.join(DEMO_DIR, "matches.csv")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser(
        description='Demo which reads in two files and combines them to form "Hello world!"'
    )
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    api_url = parser.parse_args().api_url

    # Required files
    if not os.path.exists(MESSAGES_CSV):
        raise Exception('Input CSV file {} not found', MESSAGES_CSV)
    if not os.path.exists(RECORDS_CSV):
        raise Exception('Input CSV file {} not found', RECORDS_CSV)
    if not os.path.exists(COMBINER_SQL):
        raise Exception('Input SQL file {} not found', COMBINER_SQL)
    with open(MATCHES_CSV, "w+") as f_out:
        f_out.write("")

    # Create program
    program_name = "demo-hello-world-program"
    program_sql = open(COMBINER_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "",
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
    for (connector_name, stream, filepath, is_input) in [
        ("demo-hello-world-messages", "MESSAGES", MESSAGES_CSV, True),
        ("demo-hello-world-records", "RECORDS", RECORDS_CSV, True),
        ("demo-hello-world-matches", "MATCHES", MATCHES_CSV, False),
    ]:
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "transport": {
                    "name": "file_" + ("input" if is_input else "output"),
                    "config": {
                        "path": filepath,
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            },
        }).raise_for_status()
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

    # Create pipeline
    pipeline_name = "demo-hello-world-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "description": "",
        "config": {"workers": 6},
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()

    # Start pipeline
    print("(Re)starting pipeline...")
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Shutdown":
        time.sleep(1)
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Running":
        time.sleep(1)
    print("Pipeline (re)started")

    # Wait till the pipeline is completed
    while not requests.get(f"{api_url}/v0/pipelines/{pipeline_name}/stats") \
            .json()["global_metrics"]["pipeline_complete"]:
        time.sleep(1)
    print("Pipeline completed")

    # Verify result
    with open(MATCHES_CSV) as f:
        content = f.read().strip()
        assert content == "Hello world!,1"
        print(content[:-2])


if __name__ == "__main__":
    main()
