# Part 3. Backfilling & Input Orchestration

This article demonstrates how to seamlessly transition from backfilling
historical data to processing live input data. We do this by orchestrating the
input connectors to control the timing and order of data ingestion from multiple
sources. Specifically, we do this by:

- Assigning **labels** to the connectors based on their role.
- Configuring the live input connector to **start_after** the historical input
  connector.

In this example, we will configure the `LINEITEM` table to first ingest
historical data from the Delta Lake connector and then ingest live data from a
different data source.

## Simulating Real Time Data

To simulate live data, we use Feldera's built-in data generator. We
assign the Delta Lake connector the label *lineitem.historical* and configure
the datagen conector to **start_after** all historical data has been ingested.

This setup ensures that generated live data starts flowing only when the
backfill has completed.


```json
CREATE TABLE LINEITEM (...) WITH (
 'connectors' = '[{
    "labels": ["lineitem.historical"],
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/lineitem",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 },
 {
    "labels": ["lineitem.datagen"],
    "start_after": "lineitem.historical",
    "transport": {
	  "name": "datagen",
	  "config": {
            "plan": [{
              "fields": {
                "L_PARTKEY": {"strategy": "zipf", "range": [1, 25]},
                "L_LINENUMBER": {"strategy": "zipf", "range": [1, 25]},
                "L_RETURNFLAG": {"values": ["A", "N", "R"]},
                "L_LINESTATUS": {"values": ["F", "0", "F", "F"]},
                "L_QUANTITY": {"range": [0.01, 99.99]},
                "L_EXTENDEDPRICE": {"range": [0.02, 99.99]},
                "L_DISCOUNT": {"range": [0.01, 99.99]},
                "L_TAX": {"range": [0.01, 99.99]}
              }
            }]
	  }
    }
 }]'
);
```

## Attempt 1: Using Kafka to Ingest Real Time Data

For a more realistic real-time data ingestion scenario, let's replace the data
generator with a Kafka based input connector. Here, historical data is first
loaded from Delta Lake before switching to real time data from Kafka.

By ensuring that Kafka connector **starts after** historical data has been
ingested, we prevent any duplicate records.

```json
 {
    "labels": ["lineitem.live"],
    "start_after": "lineitem.historical",
    "transport": {
        "name": "kafka_input",
        "config": {
            "bootstrap.servers": "redpanda:9092",
            "auto.offset.reset": "latest",
            "topics": ["lineitem"]
        }
    },
    "format": {
        "name": "csv",
        "config": {
            "delimiter": "|"
        }
    }
 }
```

### Ensuring Data Consistency with `auto.offset.reset`

In the Kafka connector, we configure **auto.offset.reset** to **latest**. This
ensures that only the latest messages arriving after the consumer starts are
ingested, preventing redundancy.

If **auto.offset.reset** is set to **earliest**, all available messages from the
beginning of the topic, including older historical data, that may have already
been loaded from Delta Lake, will be processed.

:::danger
- Ensure all historical data in Kafka is moved to Delta Lake before Feldera
  starts reading.
- Failure to do so **may result in data loss** due to missing records not
  included in the backfill process.
:::

## Recommended Approach: Kafka and Delta Lake

To avoid data loss, we recommend:
- **Track Kafka Offsets**: Monitor the Kafka offset up to which data has been
  successfully backed up to Delta Lake.
- **Specify Offset in Kafka Connector Configuration**: In the Kafka input
  connector configuration, specify the offset to start reading from. This
  ensures a smooth transition from backfilled to real-time data.

For this example, we assume that our Kafka instance has a single partition
**(0)** and that data has been synced to Delta Lake up to **offset 41**.
Therefore, we want Feldera to start reading from **offset 42**.

```json
  {
    "labels": ["lineitem.live"],
    "start_after": "lineitem.historical",
    "transport": {
        "name": "kafka_input",
        "config": {
            "bootstrap.servers": "localhost:9092",
            "topics": ["lineitem"],
            "seek": [{
              "topic": "lineitem",
              "partition": 0,
              "offset": 42
            }]
        }
    },
    "format": {
        "name": "json",
        "config": {
            "update_format": "insert_delete",
            "array": false
        }
    }
 }
```

In cases with **multiple partitions and multiple topics**, it is necessary to
**specify the offset for each partition and topic**.

## Takeaways

- Configure the live input connector to **start_after** the historical input
  connector.
- When using **Apache Kafka for real time** data alongside a  historical data
  source, specify the **offset** to start reading from.

