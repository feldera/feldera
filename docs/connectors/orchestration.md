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
[pipeline statistics endpoint](/api/retrieve-pipeline-statistics-e-g-metrics-performance-counters).

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
   fda table-connector example numbers c1 start
   ```
   Now the Changes Stream tab will show new input records from both connectors.

4. Pause connector `c2`:
   ```
   fda table-connector example numbers c2 pause
   ```
   Now the Changes Stream tab no longer will show new input records from connector `c2`.

## Automatic connector orchestration

Feldera allows encoding the order of connector activation directly in the SQL program.
This mechanism can express ordering constraints of the form "start connector
C1 after connectors C1, C2, ... have finished ingesting all inputs".
While less general than the mechanism described above, it covers most
practical situations, while eliminating the need to write
scripts to monitor and manage connector status via the API.

To configure automatic connector orchestration, you need to:

1. Assign labels to connectors based on their role.
2. Set the `start_after` attribute to configure the order of connector activation.

### Labels

A connector can be assigned one or more text labels that reflect its role in the pipeline.
For example, the following label indicates that the connector is used
to backfill the pipeline with historical data.

```
"label": ["backfill"]
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
    "labels": "price.backfill",
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
            "topics": ["price"],
            "bootstrap.servers": "redpanda:9092",
            "auto.offset.reset": "earliest"
        }
    }
}]');
```