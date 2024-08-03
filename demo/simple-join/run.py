import os
import time
import requests
import argparse
import tempfile
import json

nouns = """
table,3
chair,2
lamp,1
plant,3
window,6
car,4
bicycle,2
desk,3
paper,5
glass,3
wind,7
tile,2
building,2
"""

colors = """
red,2
orange,1
yellow,3
green,8
blue,4
indigo,3
violet,5
"""

expected = {
    "table,yellow,3,1",
    "table,indigo,3,1",
    "chair,red,2,1",
    "lamp,orange,1,1",
    "plant,yellow,3,1",
    "plant,indigo,3,1",
    "car,blue,4,1",
    "bicycle,red,2,1",
    "desk,yellow,3,1",
    "desk,indigo,3,1",
    "paper,violet,5,1",
    "glass,yellow,3,1",
    "glass,indigo,3,1",
    "tile,red,2,1",
    "building,red,2,1",
}

program_sql = """
CREATE TABLE nouns (
    noun STRING,
    category INTEGER
);

CREATE TABLE colors (
    color STRING,
    category INTEGER
);

CREATE VIEW nouns_with_colors AS
    SELECT
        nouns.noun,
        colors.color,
        colors.category
    FROM
        nouns JOIN colors
        ON nouns.category = colors.category;
"""


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument("--connector-type", required=False, default='file', help="Either 'file' or 'http'")
    parsed_args = parser.parse_args()
    api_url = parsed_args.api_url
    connector_type = parsed_args.connector_type
    assert connector_type in ["file", "http"]

    # Create program
    program_name = "simple-join-program"
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "",
        "code": program_sql,
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
    path_out = None
    if connector_type == "file":
        fd_nouns, path_nouns = tempfile.mkstemp(suffix='.csv', prefix='nouns', text=True)
        with os.fdopen(fd_nouns, 'w') as f:
            f.write(nouns)
        print(f"Nouns data written to f{path_nouns}")

        fd_colors, path_colors = tempfile.mkstemp(suffix='.csv', prefix='colors', text=True)
        with os.fdopen(fd_colors, 'w') as f:
            f.write(colors)
        print(f"Colors data written to f{path_colors}")

        fd_out, path_out = tempfile.mkstemp(suffix='.csv', prefix='output', text=True)
        os.close(fd_out)

        for (connector_name, stream, filepath, is_input) in [
            ("simple-join-nouns", "NOUNS", path_nouns, True),
            ("simple-join-colors", "COLORS", path_colors, True),
            ("simple-join-nouns-with-colors", "NOUNS_WITH_COLORS", path_out, False),
        ]:
            # Create connector
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

            # Add to list of connectors
            connectors.append({
                "connector_name": connector_name,
                "is_input": is_input,
                "name": connector_name,
                "relation_name": stream
            })

    # Create pipeline
    pipeline_name = "simple-join-pipeline"
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

    if connector_type == "http":
        output = requests.post(f'{api_url}/v0/pipelines/{pipeline_name}/egress/NOUNS_WITH_COLORS',
                               stream=True)

        print("Sending nouns data")
        requests.post(f'{api_url}/v0/pipelines/{pipeline_name}/ingress/NOUNS', data=nouns).raise_for_status()

        print("Sending colors data")
        requests.post(f'{api_url}/v0/pipelines/{pipeline_name}/ingress/COLORS', data=colors).raise_for_status()

        data = set()
        for line in output.iter_lines():
            json_val = json.loads(line)
            if 'text_data' in json_val:
                for internal_line in json_val['text_data'].strip().split("\n"):
                    data.add(internal_line)
                print(sorted(list(data)))
                print(sorted(list(expected)))
                if data == expected:
                    break
        assert data == expected, "Data: %s, Expected: %s" % (data, expected)
        print("Data matches")

        print("Sending neighborhood request")
        neighborhood = requests.post(
            f'{api_url}/v0/pipelines/{pipeline_name}/egress/NOUNS_WITH_COLORS?query=neighborhood&mode=snapshot&format=csv',
            json={'before': 2, 'after': 3, 'anchor': ['rain', 'orange', 3]}
        )
        print("result: " + str(neighborhood))
        assert neighborhood.status_code == requests.codes.ok

        print("neighborhood:")
        csv = neighborhood.json()['text_data']
        print(csv.strip())

        print("Sending invalid neighborhood request")
        response = requests.post(
            f'{api_url}/v0/pipelines/{pipeline_name}/egress/NOUNS_WITH_COLORS?query=neighborhood&mode=snapshot&format=csv',
            json={'before': 3, 'after': 2, 'anchor': [123, 456, 'not_a_number']})
        print("result: " + str(response))
        assert response.status_code != requests.codes.ok
        print("response:")
        print(response.json())

        print("Sending quantiles request")
        quantiles = requests.post(
            f'{api_url}/v0/pipelines/{pipeline_name}/egress/NOUNS_WITH_COLORS?query=quantiles&mode=snapshot&format=csv&quantiles=100'
        )
        print("result: " + str(quantiles))
        assert quantiles.status_code == requests.codes.ok

        print("quantiles:")
        csv = quantiles.json()['text_data']
        print(csv.strip())

    current_status = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"]
    print("Pipeline status: " + str(current_status))

    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/pause").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Paused":
        time.sleep(1)
    print("Pipeline paused")

    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Running":
        time.sleep(1)
    print("Pipeline restarted")

    if connector_type == "file":
        # Wait till the pipeline is completed
        while not requests.get(f"{api_url}/v0/pipelines/{pipeline_name}/stats") \
                .json()["global_metrics"]["pipeline_complete"]:
            time.sleep(1)
        print("Pipeline finished")

        requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Shutdown":
            time.sleep(1)
        print("Pipeline shutdown")

        requests.delete(f"{api_url}/v0/pipelines/{pipeline_name}").raise_for_status()
        print("Pipeline deleted")

        with open(path_out, 'r') as outfile:
            output = outfile.read()
        print("Output read from '" + path_out + "':")
        print(output)
    else:
        requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Shutdown":
            time.sleep(1)
        print("Pipeline shutdown")

        requests.delete(f"{api_url}/v0/pipelines/{pipeline_name}").raise_for_status()
        print("Pipeline deleted")


if __name__ == "__main__":
    main()
