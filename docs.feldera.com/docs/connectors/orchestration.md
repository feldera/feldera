# Connector orchestration

Connector orchestration enables users to activate or deactivate connectors on
demand, giving them control over the timing and order of data ingestion from
multiple sources, and over which sinks receive the output of a pipeline. It can,
for example, be used to backfill a pipeline with historical data from a database
or data lake before switching over to real-time ingestion from a streaming
source like Kafka.

## Input connectors

Input connectors can be in either the `Running` or `Paused` state. By default,
connectors are initialized in the `Running` state when a pipeline is deployed.
In this state, the connector actively fetches data from its configured data
source and forwards it to the pipeline. If needed, a connector can be created
in the `Paused` state by setting its
[`paused`](/connectors/#generic-attributes) property
to `true`.
The current connector state can be retrieved via the
[pipeline statistics endpoint](/api/get-pipeline-metrics).

When paused, the connector remains idle until it is reactivated.
Conversely, a connector in the `Running` state can be paused at any time.
This can be done by calling its
[start/pause endpoint](/api/control-input-connector).

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

A common use case for connector orchestration is loading historical data from a database before switching over to a real-time data source such as Kafka. To implement this scenario, we need to determine when the first connector has exhausted all its inputs. This can be achieved by polling the [connector status endpoint](/api/get-input-status), which provides information about the connector's configuration and current state, including the following fields:

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
| [Iceberg](/connectors/sources/iceberg)    | yes                         | Stops after reading a complete table snapshot. |
| [Kafka](/connectors/sources/kafka)        | when `enable.partition.eof` | Otherwise, waits for new messages from the Kafka topic. |
| [Pub/Sub](/connectors/sources/pubsub)     | no                          | Waits for new messages from the Pub/Sub subscription. |
| [Postgres](/connectors/sources/postgresql)| yes                         | Stops after reading a complete table snapshot (use the [Debezium connector](/connectors/sources/debezium) for Change Data Capture). |
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
CREATE TABLE price (
    part BIGINT NOT NULL,
    vendor BIGINT NOT NULL,
    price INTEGER
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

## Output connectors

Output connectors can be paused too, which is useful when a sink is unavailable
or must not receive updates for a while: a paused output connector lets the
pipeline run on without waiting for its sink.

An output connector can be in either the `Running` or `Paused` state. In the
`Running` state, the connector writes the output of its view to the configured
sink. A paused connector discards the output it receives instead. Like an input
connector, it can be created in the `Paused` state by setting its
[`paused`](/connectors/#generic-attributes) property to `true`, and it is paused
and started at runtime with its
[start/pause endpoint](/api/control-output-connector):

```bash
fda connector my_pipeline my_view my_connector pause
fda connector my_pipeline my_view my_connector start
```

The current connector state is reported as the `paused` field of the
[connector status endpoint](/api/get-output-status) and of the
[pipeline statistics endpoint](/api/get-pipeline-metrics).

A few consequences are worth spelling out:

* Output produced while the connector is paused is gone for good. Starting the
  connector resumes it with the output the pipeline produces from that point on;
  it does not replay what it missed, so its sink can be missing updates that its
  view has already applied.

* The pipeline does not wait for a paused connector: the connector reports the
  output it discards as processed, so
  [completion tokens](/connectors/completion-tokens) and other progress
  indicators keep advancing. It does not, however, transmit any records, so its
  `transmitted_records` counter stops advancing.

* Only output the pipeline produces after the pause is discarded. Output the
  connector has already been handed still reaches the sink: the batch it is
  writing, the updates already queued for it (bounded by `max_queued_records`),
  and the contents of its output buffer, which pausing flushes. Records can
  therefore keep arriving at the sink for a while after the connector reports
  itself paused, and a connector that has already filled its queue keeps the
  pipeline waiting until that queue drains.

* A connector that is paused while the pipeline
  [bootstraps](/pipelines/modifying#bootstrapping) a new or modified view
  discards the contents that the bootstrap emits for it, and starting it again
  does not re-send them. To have a connector receive the full state of its view
  after a pause, configure it with
  [`send_snapshot`](/connectors/#generic-attributes) and recreate it, or start
  it before the bootstrap runs.

* The paused state survives a restart: it is stored in the pipeline's
  checkpoint, so a pipeline that resumes from a checkpoint comes back with the
  connectors the user left paused still paused. The checkpointed state wins
  over the `paused` property in the configuration, so editing that property
  alone has no effect on a pipeline resuming from a checkpoint; modifying the
  connector or its view resets the connector to its configured state.
