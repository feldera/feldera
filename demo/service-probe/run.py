import time
import requests
import argparse


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    api_url = parser.parse_args().api_url

    # Kafka server to create service for to probe
    pipeline_to_redpanda_server = "redpanda:9092"

    # Create a service
    print("Creating service example...")
    requests.put(f"{api_url}/v0/services/example", json={
        "description": "Example service",
        "config": {
            "kafka": {
                "bootstrap_servers": [pipeline_to_redpanda_server],
                "options": {}
            }
        }
    }).raise_for_status()

    # Probe connectivity
    print("Probing connectivity...")
    response = requests.post(f"{api_url}/v0/services/example/probes", json="test_connectivity")
    response.raise_for_status()
    probe_id = response.json()["service_probe_id"]
    while True:
        probe = requests.get(f"{api_url}/v0/services/example/probes?id={probe_id}").json()[0]
        print(f"Status: {probe['status']}")
        if probe['status'] in ["failure", "success"]:
            print(f"Connectivity test response: {probe['response']}")
            break
        time.sleep(1)

    # Probe topics
    print("Probing topics...")
    response = requests.post(f"{api_url}/v0/services/example/probes", json="kafka_get_topics")
    response.raise_for_status()
    probe_id = response.json()["service_probe_id"]
    while True:
        probe = requests.get(f"{api_url}/v0/services/example/probes?id={probe_id}").json()[0]
        print(f"Status: {probe['status']}")
        if probe['status'] in ["failure", "success"]:
            print(f"Get topics response: {probe['response']}")
            break
        time.sleep(1)

    # Retrieve all probes
    print("Fetching all probes...")
    probes = requests.get(f"{api_url}/v0/services/example/probes").json()
    num_pending = sum(map(lambda p: 1 if p['status'] == 'pending' else 0, probes))
    num_running = sum(map(lambda p: 1 if p['status'] == 'running' else 0, probes))
    num_success = sum(map(lambda p: 1 if p['status'] == 'success' else 0, probes))
    num_failure = sum(map(lambda p: 1 if p['status'] == 'failure' else 0, probes))
    print(f"There are in total {len(probes)} probes:")
    print(f"  > Status pending: {num_pending}")
    print(f"  > Status running: {num_running}")
    print(f"  > Status success: {num_success}")
    print(f"  > Status failure: {num_failure}")

    # Retrieve all probes
    print("Retrieving filtered list of probes...")
    print("Latest test_connectivity probe: %s" % requests.get(f"{api_url}/v0/services/example/probes?limit=1&type=test_connectivity").json())
    print("Latest kafka_get_topics probe: %s" % requests.get(f"{api_url}/v0/services/example/probes?limit=1&type=kafka_get_topics").json())


if __name__ == "__main__":
    main()
