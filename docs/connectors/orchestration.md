# Input connector orchestration

Connector Orchestration enables users to activate or deactivate input connectors
on demand, giving them control over the timing and order of data ingestion from
multiple sources. It can, for example, be used to backfill a pipeline with
historical data from a database or data lake before switching over to real-time
ingestion from a streaming source like Kafka.

Input connectors can be in either the `Running` or `Paused` state. By default,
connectors are initialized in the `Running` state when a pipeline is deployed.
In this state, the connector actively fetches data from its configured data
source and forwards it to the pipeline. If needed, a connector can be created
in the `Paused` state by setting its
[`paused`](/connectors/#generic-attributes) property
to `true`.
The current connector state can be retrieved via the
[pipeline statistics endpoint](/api/retrieve-circuit-metrics-of-a-running-or-paused-pipeline).

When paused, the connector remains idle until it is reactivated.
Conversely, a connector in the `Running` state can be paused at any time.
This can be done by calling its
[start/pause endpoint](/api/start-resume-or-pause-the-input-connector).

Note that only if both the pipeline *and* the connector state is `Running`,
is the input connector active. The following table illustrates this:
```text
Pipeline state    Connector state    Connector is active?
--------------    ---------------    --------------------
Paused            Paused             No
Paused            Running            No
Running           Paused             No
Running           Running            Yes
```

## Orchestration example

1. Create and start a pipeline named `example` with the following SQL:
   ```sql
   CREATE TABLE numbers (
     num INT
   ) WITH (
       'connectors' = '[
           {
               "name": "c1",
               "paused": true,
               "transport": {
                   "name": "datagen",
                   "config": {"plan": [{ "rate": 1, "fields": { "num": { "range": [0, 10], "strategy": "uniform" } } }]}
               }
           },
           {
               "name": "c2",
               "paused": false,
               "transport": {
                   "name": "datagen",
                   "config": {"plan": [{ "rate": 1, "fields": { "num": { "range": [10, 20], "strategy": "uniform" } } }]}
               }
           }
       ]'
   );
   ```

   Note that the `numbers` table has two input connectors, one of which has `paused` property set to `true`.
   This connector will be created in the `Paused` state.

2. Check the `numbers` table checkbox in the Change Stream tab. Observe that although the pipeline is `Running`,
   the change stream only shows input records from connector `c2` (i.e., `[10, 20)`) but not of connector
   `c1` (i.e., `[0, 10)`).

3. Start connector `c1`:
   ```
   fda connector example numbers c1 start
   ```
   Now the Changes Stream tab will show new input records from both connectors.

4. Pause connector `c2`:
   ```
   fda connector example numbers c2 pause
   ```
   Now the Changes Stream tab no longer will show new input records from connector `c2`.

## Detecting when a connector has finished ingesting data

A common use case for connector orchestration is loading historical data from a database before switching over to a real-time data source such as Kafka. To implement this scenario, we need to determine when the first connector has exhausted all its inputs. This can be achieved by polling the [connector status endpoint](/api/retrieve-the-status-of-an-input-connector), which provides information about the connector's configuration and current state, including the following fields:

```json
{
  "endpoint_name": "project_memberships.datagen",
  "config": {...},
  "metrics": {
    "buffered_records": 0,
    "end_of_input": false,
    ...
  },
  ...
}
```

* `end_of_input`: Indicates that the connector has received all available inputs from its data source and will not produce any more.
* `buffered_records`: Tracks the number of input records received by the connector that have not been ingested by the pipeline yet.

Once `end_of_input` is true and `buffered_records` is 0, the pipeline will no longer receive any new inputs from the connector:

```bash
fda connector my_pipeline my_table my_connector stats | jq '.metrics.end_of_input == true and .metrics.buffered_records == 0'
```

Not all connectors reach the end of input. Some, like Pub/Sub, continuously wait for new data. Others signal the end of input depending on their configuration. The following table summarizes the end-of-input behavior for different input connectors:


| Connector  | Signals end-of-input         | Comment |
|------------|------------------------------|---------|
| [HTTP GET](/connectors/sources/http-get)  | yes                         |         |
| [Datagen](/connectors/sources/datagen)    | when `limit` is set         | The Datagen connector stops producing inputs after reaching the specified record limit. |
| [Debezium](/connectors/sources/debezium)  | no                          |         |
| [Delta Lake](/connectors/sources/delta)   | when `mode=snapshot`        | When configured with `mode=snapshot`, the DeltaLake connector signals the end of input after ingesting the specified snapshot of the table. |
| File                                      | when `follow=false`         | When configured with `follow=false` (the default), the file input connector signals the end of input after reading the current contents of the file; otherwise (`follow=true`), the connector continues polling for new changes. |
| [Iceberg](/connectors/sources/iceberg)    | yes                         | Stops after reading a complete table shapshot. |
| [Kafka](/connectors/sources/kafka)        | when `enable.partition.eof` | Otherwise, waits for new messages from the Kafka topic. |
| [Pub/Sub](/connectors/sources/pubsub)     | no                          | Waits for new messages from the Pub/Sub subscriptio. |
| [Postgres](/connectors/sources/postgresql)| yes                         | Stops after reading a complete table shapshot (use the [Debezium connector](/connectors/sources/debezium) for Change Data Capture). |
| [S3](/connectors/sources/s3)              | yes                         | Stops after reading all objects that match the specified prefix. |


## Automatic connector orchestration

Feldera allows encoding the order of connector activation directly in the SQL program.
This mechanism can express ordering constraints of the form "start connector
C1 after connectors C1, C2, ... have finished ingesting all inputs".
While less general than the mechanism described above, it covers most
practical situations, while eliminating the need to write
scripts to monitor and manage connector status via the API.

To configure automatic connector orchestration:

1. Assign labels to connectors based on their role.
2. Set the `start_after` attribute to configure the order of connector activation.

### Labels

A connector can be assigned one or more text labels that reflect its role in the pipeline.
For example, the following label indicates that the connector is used
to backfill the pipeline with historical data.

```
"labels": ["backfill"]
```

### Configuring the order of connector activation using `start_after`

A connector can be configured with a `start_after` attribute, which specifies
one or more labels, e.g.:

```
"start_after": "backfill"
```

or

```
"start_after": ["label1",  "label2"]
```

Such a connector is created in the Paused state and is automatically activated once
all connectors tagged with at least one of the specified labels have reached the end of input.

### Example

The Feldera Basics tutorial gives an example of a
[table with two input connectors](/tutorials/basics/part3#configure-connectors).
The following snippet shows a modified version of this example where the
second connector is configured to start after the first connector completes:

```sql
create table PRICE (
    part bigint not null,
    vendor bigint not null,
    price integer
) WITH ('connectors' = '[{
    "labels": ["price.backfill"],
    "transport": {
        "name": "url_input", "config": {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"  }
    },
    "format": { "name": "json" }
},
{
    "start_after": ["price.backfill"],
    "format": {"name": "json"},
    "transport": {
        "name": "kafka_input",
        "config": {
            "topic": "price",
            "start_from": "earliest",
            "bootstrap.servers": "redpanda:9092"
        }
    }
}]');
```
