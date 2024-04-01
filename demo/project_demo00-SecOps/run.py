import os
import time
import requests
import argparse
import subprocess
from shutil import which
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument("--prepare-args", required=False, help="number of SecOps pipelines to simulate")
    args = parser.parse_args()
    prepare_feldera(args.api_url)
    prepare_redpanda_start_simulator("-1" if args.prepare_args is None else args.prepare_args)


def prepare_feldera(api_url):
    pipeline_to_redpanda_server = "redpanda:9092"

    # Create program
    program_name = "demo-sec-ops-program"
    program_sql = open(PROJECT_SQL).read()
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
    for (connector_name, stream, topic_topics, is_input) in [
        ("secops_pipeline", 'PIPELINE',  ["secops_pipeline"], True),
        ("secops_pipeline_sources", 'PIPELINE_SOURCES', ["secops_pipeline_sources"], True),
        ("secops_artifact", 'ARTIFACT', ["secops_artifact"], True),
        ("secops_vulnerability", 'VULNERABILITY', ["secops_vulnerability"], True),
        ("secops_cluster", 'K8SCLUSTER', ["secops_cluster"], True),
        ("secops_k8sobject", 'K8SOBJECT', ["secops_k8sobject"], True),
        ("secops_vulnerability_stats", 'K8SCLUSTER_VULNERABILITY_STATS', "secops_vulnerability_stats", False),
    ]:
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                },
                "transport": {
                    "name": "kafka_" + ("input" if is_input else "output"),
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": topic_topics
                    } if not is_input else (
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest",
                            "group.id": "secops_pipeline_sources",
                            "enable.auto.commit": "true",
                            "enable.auto.offset.store": "true",
                        }
                        if stream == "PIPELINE_SOURCES" else
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest"
                        }
                    )
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

    # Create pipeline
    pipeline_name = "demo-sec-ops-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "description": "",
        "config": {"workers": 8},
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()


def prepare_redpanda_start_simulator(num_pipelines):
    # Create output topic before running the simulator, which will never return.
    print("(Re-)creating topic secops_vulnerability_stats...")
    rpk["topic", "delete", "secops_vulnerability_stats"]()
    rpk[
        "topic",
        "create",
        "secops_vulnerability_stats",
        "-c",
        "retention.ms=-1",
        "-c",
        "retention.bytes=100000000",
    ]()
    print("(Re-)created topic secops_vulnerability_stats")

    # Start running the simulator
    if which("cargo") is None:
        # Expect a pre-built binary in simulator/secops_simulator. Used
        # by the Docker container workflow where we don't want to use cargo run.
        cmd = ["./secops_simulator", "%s" % num_pipelines]
        subprocess.run(cmd, cwd=os.path.join(SCRIPT_DIR, "simulator"))
    else:
        cmd = ["cargo", "run", "--release", "--", "%s" % num_pipelines]
        # Override --release if RUST_BUILD_PROFILE is set
        if "RUST_BUILD_PROFILE" in os.environ:
            cmd[2] = os.environ["RUST_BUILD_PROFILE"]
        new_env = os.environ.copy()
        new_env["RUST_LOG"] = "debug"
        subprocess.run(cmd, cwd=os.path.join(SCRIPT_DIR, "simulator"), env=new_env)


if __name__ == "__main__":
    main()
