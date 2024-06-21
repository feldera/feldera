#! /usr/bin/python3

import csv
import os
import sys
import time
import requests
import argparse

# File locations
DEMO_DIR = os.path.join(os.path.dirname(__file__))
NEXMARK_SQL = os.path.join(DEMO_DIR, "nexmark.sql")

def table_sql(with_lateness):
    if with_lateness:
        lateness = "LATENESS INTERVAL 4 SECONDS"
    else:
        lateness = ""
    return f"""
CREATE TABLE person (
    id BIGINT,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP(3) NOT NULL {lateness},
    extra  VARCHAR
);
CREATE TABLE auction (
    id  BIGINT,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    date_time  TIMESTAMP(3) NOT NULL {lateness},
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    extra  VARCHAR
);
CREATE TABLE bid (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    date_time TIMESTAMP(3) NOT NULL {lateness},
    extra  VARCHAR
);
"""

QUERY_SQL = {
    'q0': """CREATE VIEW q0 AS SELECT auction, bidder, price, date_time, extra FROM bid;""",

    'q1': """CREATE VIEW q1 AS
SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    date_time,
    extra
FROM bid;""",

    'q2': """CREATE VIEW q2 AS SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;""",

    'q3': """CREATE VIEW q3 AS SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');""",

    'q4': """CREATE VIEW q4 AS
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;""",

    'q7': """CREATE VIEW q7 AS
SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
from bid B
JOIN (
  SELECT MAX(B1.price) AS maxprice, TUMBLE_START(B1.date_time, INTERVAL '10' SECOND) as date_time
  FROM bid B1
  GROUP BY TUMBLE(B1.date_time, INTERVAL '10' SECOND)
) B1
ON B.price = B1.maxprice
WHERE B.date_time BETWEEN B1.date_time  - INTERVAL '10' SECOND AND B1.date_time;""",

    'q8': """CREATE VIEW q8 AS
SELECT P.id, P.name, P.starttime
FROM (
  SELECT P.id, P.name,
         TUMBLE_START(P.date_time, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(P.date_time, INTERVAL '10' SECOND) AS endtime
  FROM person P
  GROUP BY P.id, P.name, TUMBLE(P.date_time, INTERVAL '10' SECOND)
) P
JOIN (
  SELECT A.seller,
         TUMBLE_START(A.date_time, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(A.date_time, INTERVAL '10' SECOND) AS endtime
  FROM auction A
  GROUP BY A.seller, TUMBLE(A.date_time, INTERVAL '10' SECOND)
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;""",

    'q9': """CREATE VIEW q9 AS
SELECT
    id, itemName, description, initialBid, reserve, date_time, expires, seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
   SELECT A.*, B.auction, B.bidder, B.price, B.date_time AS bid_dateTime, B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.date_time ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
)
WHERE rownum <= 1;""",

    'q10': """CREATE VIEW q10 AS -- PARTITIONED BY (dt, hm) AS
SELECT auction, bidder, price, date_time, extra, FORMAT_DATE('yyyy-MM-dd', date_time), FORMAT_DATE('HH:mm', date_time)
FROM bid;""",

    'q15': """CREATE VIEW q15 AS
SELECT
     FORMAT_DATE('yyyy-MM-dd', date_time) as 'day',
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     count(distinct bidder) AS total_bidders,
     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
     count(distinct auction) AS total_auctions,
     count(distinct auction) filter (where price < 10000) AS rank1_auctions,
     count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
     count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY FORMAT_DATE('yyyy-MM-dd', date_time);""",

    'q17': """CREATE VIEW q17 AS
SELECT
     auction,
     format_date('yyyy-MM-dd', date_time) as 'day',
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     min(price) AS min_price,
     max(price) AS max_price,
     avg(price) AS avg_price,
     sum(price) AS sum_price
FROM bid
GROUP BY auction, format_date('yyyy-MM-dd', date_time);""",

    'q18': """CREATE VIEW q18 AS
SELECT auction, bidder, price, channel, url, date_time, extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1;""",

    'q19': """CREATE VIEW q19 AS
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10;""",

    'q20': """CREATE VIEW q20 AS
SELECT
    auction, bidder, price, channel, url, B.date_time, B.extra,
    itemName, description, initialBid, reserve, A.date_time as AdateTime, expires, seller, category, A.extra as Aextra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;""",
}

def sort_queries(queries):
    return sorted(queries, key=lambda q: int(q[1:]))
    
def parse_queries(arg):
    if arg is not None:
        queries = set()
        for s in arg:
            for q in s.split(','):
                q = q.lower()
                if q == 'all':
                    queries = set(QUERY_SQL.keys())
                elif q in QUERY_SQL:
                    queries.add(q)
                elif q != '':
                    sys.stderr.write(f'unknown query {q}\n')
                    sys.exit(1)
    else:
        queries = set(QUERY_SQL.keys())

    if len(queries) == 0:
        sys.stderr.write('no queries specified\n')
        sys.exit(1)

    return queries

def add_connector(connector_name, relation_name, is_input):
    transport_type = "kafka_" + ("input" if is_input else "output")
    json = {
        "description": "",
        "config": {
            "transport": {
                "name": transport_type,
                "config": {
                    "auto.offset.reset": "earliest",
                    "bootstrap.servers": kafka_broker,
                    "enable.ssl.certificate.verification": "true",
                    "sasl.mechanism": "PLAIN",
                    "security.protocol": "PLAINTEXT",
                }
            },
            "format": {
                "name": "csv",
                "config": {}
            }
        }
    }
    config = json["config"]["transport"]["config"]
    if is_input:
        config["enable.partition.eof"] = "true"
        config["topics"] = [connector_name]
    else:
        config["topic"] = connector_name
    requests.put(f"{api_url}/v0/connectors/{connector_name}", json=json).raise_for_status()
    return {
        "connector_name": connector_name,
        "is_input": is_input,
        "name": connector_name,
        "relation_name": relation_name,
    }
    
def add_input_connector(connector_name, relation_name):
    return add_connector(connector_name, relation_name, True)

def add_output_connector(connector_name, relation_name):
    return add_connector(connector_name, relation_name, False)

def stop_pipeline(pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
    if wait:
        return wait_for_status(pipeline_name, "Shutdown")

def start_pipeline(pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    if wait:
        return wait_for_status(pipeline_name, "Running")

def wait_for_status(pipeline_name, status):
    start = time.time()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != status:
        time.sleep(.1)
    return time.time() - start

def write_results(results, outfile):
    writer = csv.writer(outfile)
    writer.writerow(['when', 'runner', 'mode', 'language', 'name', 'num_cores', 'num_events', 'elapsed', 'peak_memory_bytes'])
    writer.writerows(results)
    
def write_metrics(keys, results, outfile):
    writer = csv.writer(outfile)
    sorted_keys = sorted(keys)
    writer.writerow(sorted_keys)
    for row_dict in results:
        row_list = []
        for key in sorted_keys:
            if key in row_dict:
                row_list += [row_dict[key]]
            else:
                row_list += [""]
        writer.writerow(row_list)

def main():
    # Command-line arguments
    parser = argparse.ArgumentParser(
        description='Nexmark benchmark demo'
    )
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument("--kafka-broker", required=True, help="Kafka broker (e.g., localhost:9092 )")
    parser.add_argument("--cores", type=int, help="Number of cores to use for workers (default: 16)")
    parser.add_argument('--lateness', action=argparse.BooleanOptionalAction, help='whether to use lateness for GC to save memory (default: --lateness)')
    parser.add_argument('--merge', action=argparse.BooleanOptionalAction, help='whether to merge all the queries into one program (default: --no-merge)')
    parser.add_argument('--storage', action=argparse.BooleanOptionalAction, help='whether to enable storage (default: --no-storage)')
    parser.add_argument('--min-storage-rows', type=int, help='If storage is enabled, the minimum number of rows to write a batch to storage.')
    parser.add_argument('--query', action='append', help='queries to run (by default, all queries), specify one or more of: ' + ','.join(sort_queries(QUERY_SQL.keys())))
    parser.add_argument('--input-topic-suffix', help='suffix to apply to input topic names (by default, "")')
    parser.add_argument('--csv', help='File to write results in .csv format')
    parser.add_argument('--csv-metrics', help='File to write pipeline metrics (memory, disk) in .csv format')
    parser.add_argument('--metrics-interval', help='How often metrics should be sampled, in seconds (default: 1)')
    parser.set_defaults(lateness=True, merge=False, storage=False, cores=16, metrics_interval=1)
    
    global api_url, kafka_broker
    api_url = parser.parse_args().api_url
    kafka_broker = parser.parse_args().kafka_broker
    with_lateness = parser.parse_args().lateness
    merge = parser.parse_args().merge
    queries = sort_queries(parse_queries(parser.parse_args().query))
    cores = int(parser.parse_args().cores)
    storage = parser.parse_args().storage
    min_storage_rows = parser.parse_args().min_storage_rows
    if min_storage_rows is not None:
        min_storage_rows = int(min_storage_rows)
    suffix = parser.parse_args().input_topic_suffix or ''
    csvfile = parser.parse_args().csv
    csvmetricsfile = parser.parse_args().csv_metrics
    metricsinterval = float(parser.parse_args().metrics_interval)

    output_connector_names = queries
    if merge and len(queries) > 1:
        merged_name = ','.join(queries)
        QUERY_SQL[merged_name] = '\n'.join([QUERY_SQL[q] for q in queries])
        queries = [merged_name]

    when = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time()))

    for program_name in queries:
        # Create program
        program_sql = table_sql(with_lateness) + QUERY_SQL[program_name]
        response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
            "description": f"Nexmark benchmark: {program_name}",
            "code": program_sql
        })
        response.raise_for_status()
        program_version = response.json()["version"]

        # Compile program
        requests.post(f"{api_url}/v0/programs/{program_name}/compile", json={"version": program_version}).raise_for_status()
        
    print(f"Compiling program(s)...")
    for program_name in queries:
        while True:
            status = requests.get(f"{api_url}/v0/programs/{program_name}").json()["status"]
            print(f"Program {program_name} status: {status}")
            if status == "Success":
                break
            elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
                raise RuntimeError(f"Failed program compilation with status {status}")
            time.sleep(5)

    input_connectors = [add_input_connector(s + suffix, s) for s in ("auction", "bid", "person")]
    output_connectors = {}
    for name in output_connector_names:
        output_connectors[name] = add_output_connector(name, name)

    # Create pipelines
    print("Creating pipeline(s)...")
    for program_name in queries:
        pipeline_name = program_name
        requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
            "description": "",
            "config": {"workers": cores, "storage": storage, "min_storage_rows": min_storage_rows},
            "program_name": program_name,
            "connectors": input_connectors + [output_connectors[s] for s in program_name.split(',')],
        }).raise_for_status()

    # Stop pipelines
    print("Stopping pipeline(s)...")
    for pipeline_name in queries:
        stop_pipeline(pipeline_name, False)
    for pipeline_name in queries:
        stop_pipeline(pipeline_name, True)

    # Run the pipelines
    results = []
    histogram_values = ["count", "sample", "minimum", "maximum", "mean"]
    pipeline_metrics = []
    metrics_seen = {"name", "elapsed_seconds"}
    for pipeline_name in queries:
        start = time.time()

        # Start pipeline
        elapsed = start_pipeline(pipeline_name, True)
        print(f"Started pipeline {pipeline_name} in {elapsed:.1f} s")

        # Wait till the pipeline is completed
        start = time.time()
        last_processed = 0
        last_metrics = 0
        peak_memory = 0
        while True:
            stats = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}/stats").json()
            elapsed = time.time() - start
            if "global_metrics" in stats:
                global_metrics = stats["global_metrics"]
                processed = global_metrics["total_processed_records"]
                peak_memory = max(peak_memory, global_metrics["rss_bytes"])
                if processed > last_processed:
                    before, after = ('\r', '') if os.isatty(1) else ('', '\n')
                    sys.stdout.write(f"{before}Pipeline {pipeline_name} processed {processed} records in {elapsed:.1f} seconds{after}")
                last_metrics = elapsed
                metrics_dict = {"name":pipeline_name, "elapsed_seconds":elapsed}
                for key, value in global_metrics.items():
                    metrics_seen.add(key)
                    metrics_dict[key] = value
                for s in stats["metrics"]:
                    key = s["key"].replace(".", "_")
                    value = s["value"]
                    if "Counter" in value and value["Counter"] is not None:
                        metrics_seen.add(key)
                        metrics_dict[key] = value["Counter"]
                    elif "Gauge" in value and value["Gauge"] is not None:
                        metrics_seen.add(key)
                        metrics_dict[key] = value["Gauge"]
                        
                    if "Histogram" in value and value["Histogram"] is not None:
                        for v in histogram_values:
                            k = key + "_histogram_" + v
                            metrics_seen.add(k)
                            metrics_dict[k] = value["Histogram"][v]
                pipeline_metrics += [metrics_dict]
                last_processed = processed
                if stats["global_metrics"]["pipeline_complete"]:
                    break
            time.sleep(metricsinterval)
        if os.isatty(1):
            print()
        elapsed = "{:.1f}".format(time.time() - start)
        print(f"Pipeline {pipeline_name} completed in {elapsed} s")

        results += [[when, "feldera", "stream", "sql", pipeline_name, cores, last_processed, elapsed, peak_memory]]

        # Start pipeline
        elapsed = stop_pipeline(pipeline_name, True)
        print(f"Stopped pipeline {pipeline_name} in {elapsed:.1f} s")
    
    write_results(results, sys.stdout)
    if csvfile is not None:
        write_results(results, open(csvfile, 'w', newline=''))
    if csvmetricsfile is not None:
        write_metrics(metrics_seen, pipeline_metrics, open(csvmetricsfile, 'w', newline=''))

if __name__ == "__main__":
    main()
