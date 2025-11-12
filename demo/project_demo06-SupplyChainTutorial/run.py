import os
import time
import requests
import argparse
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")


def find_endpoint(endpoints, name):
    for endpoint in endpoints:
        if endpoint.get("endpoint_name") == name:
            return endpoint
    return None


def build_sql(pipeline_to_redpanda_server: str) -> str:
    return f"""-- SQL program for the Feldera Basics tutorial: https://docs.feldera.com/tutorials/basics/

create table VENDOR (
    id bigint not null primary key,
    name varchar,
    address varchar
) WITH ('connectors' = '[{{
    "name": "vendor",
    "transport": {{
        "name": "url_input", "config": {{"path": "https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json"}}
    }},
    "format": {{ "name": "json" }}
}}]');

create table PART (
    id bigint not null primary key,
    name varchar
) WITH ('connectors' = '[{{
    "name": "part",
    "transport": {{
        "name": "url_input", "config": {{"path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"  }}
    }},
    "format": {{ "name": "json" }}
}}]');

create table PRICE (
    part bigint not null,
    vendor bigint not null,
    price decimal
) WITH ('connectors' = '[{{
    "name": "tutorial-price-s3",
    "transport": {{
        "name": "url_input", "config": {{"path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"  }}
    }},
    "format": {{ "name": "json" }}
}},
{{
    "name": "tutorial-price-redpanda",
    "paused": true,
    "format": {{"name": "json"}},
    "transport": {{
        "name": "kafka_input",
        "config": {{
            "topics": ["price"],
            "bootstrap.servers": "{pipeline_to_redpanda_server}",
            "start_from": "earliest"
        }}
    }}
}}]');

-- Lowest available price for each part across all vendors.
create view LOW_PRICE (
    part,
    price
) as
    select part, MIN(price) as price from PRICE group by part;

-- Lowest available price for each part along with part and vendor details.
create view PREFERRED_VENDOR (
    part_id,
    part_name,
    vendor_id,
    vendor_name,
    price
)
WITH (
    'connectors' = '[{{
        "name": "preferred_vendor",
        "format": {{"name": "json"}},
        "transport": {{
            "name": "kafka_output",
            "config": {{
                "topic": "preferred_vendor",
                "bootstrap.servers": "{pipeline_to_redpanda_server}"
            }}
        }}
    }}]'
)
as
    select
        PART.id as part_id,
        PART.name as part_name,
        VENDOR.id as vendor_id,
        VENDOR.name as vendor_name,
        PRICE.price
    from
        PRICE,
        PART,
        VENDOR,
        LOW_PRICE
    where
        PRICE.price = LOW_PRICE.price AND
        PRICE.part = LOW_PRICE.part AND
        PART.id = PRICE.part AND
        VENDOR.id = PRICE.vendor;"""


def main():
    parser = argparse.ArgumentParser(
        description="Demo tutorial combining supply chain concepts (e.g., price, vendor, part) and "
        "generating insights (e.g, lowest price, preferred vendor)"
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="Feldera API URL (e.g., http://localhost:8080 )",
    )
    parser.add_argument(
        "--start", action="store_true", default=False, help="Start the Feldera pipeline"
    )
    parser.add_argument(
        "--kafka-url",
        required=False,
        default="redpanda:9092",
        help="Kafka URL reachable from the pipeline",
    )
    args = parser.parse_args()
    api_url = args.api_url
    start_pipeline = args.start
    pipeline_to_redpanda_server = args.kafka_url

    # Kafka topics
    print("(Re-)creating topics price and preferred_vendor...")
    rpk["topic", "delete", "price"]()
    rpk["topic", "create", "price"]()
    rpk["topic", "delete", "preferred_vendor"]()
    rpk["topic", "create", "preferred_vendor"]()
    print("(Re-)created topics price and preferred_vendor")

    # Create pipeline
    pipeline_name = "demo-supply-chain-tutorial-pipeline"
    requests.put(
        f"{api_url}/v0/pipelines/{pipeline_name}",
        json={
            "name": pipeline_name,
            "description": "Supply Chain Tutorial demo pipeline",
            "runtime_config": {"workers": 8},
            "program_config": {},
            "program_code": build_sql(pipeline_to_redpanda_server),
        },
    ).raise_for_status()

    # Compile program
    print("Compiling program ...")
    while True:
        status = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()[
            "program_status"
        ]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif (
            status != "Pending"
            and status != "CompilingRust"
            and status != "SqlCompiled"
            and status != "CompilingSql"
        ):
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(2)

    # Start pipeline
    if start_pipeline:
        print("(Re)starting pipeline...")
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/stop?force=true"
        ).raise_for_status()
        while (
            requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()[
                "deployment_status"
            ]
            != "Stopped"
        ):
            time.sleep(1)
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/start"
        ).raise_for_status()
        while (
            requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()[
                "deployment_status"
            ]
            != "Running"
        ):
            time.sleep(1)
        print("Pipeline (re)started")

        # Wait for the `tutorial-price-s3` connector to reach end of input
        # before enabling `tutorial-price-redpanda.`
        print("Waiting for 'tutorial-price-s3' connector to finish reading")
        while (
            find_endpoint(
                requests.get(f"{api_url}/v0/pipelines/{pipeline_name}/stats").json()[
                    "inputs"
                ],
                "price.tutorial-price-s3",
            )["metrics"]["end_of_input"]
            != True
        ):
            time.sleep(1)
        print("Starting the 'tutorial-price-redpanda' connector")
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/tables/price/connectors/tutorial-price-redpanda/start"
        ).raise_for_status()


if __name__ == "__main__":
    main()
