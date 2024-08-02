# Part 4: Random Data Generation

This is the final part of the tutorial where we will

- Learn to connect Feldera pipelines to a random row generator for testing,
  benchmarking and debugging purposes.

- Give a glimpse of the HTTP API to programmatically interact with Feldera.

You can skip this part if you are working with pre-existing data sources.

## Why random data?

When creating a new pipeline, you might find yourself writing some SQL, without having
any data to test it readily available. In this case, you can use the random data generator
to create test data on the fly. This is especially useful when you want to test with
large volumes of data.

## Step 1. Create datagen connectors

You already learned how to create connectors and connect them to your pipeline in the previous
parts of the tutorial. The datagen connector is just another connector that generates random
rows for a table with some constraints on what gets generated based on the configuration you provide.

Let's bring back the SQL table definitions for vendors from Part 1:

```sql
create table VENDOR (
    id bigint not null primary key,
    name varchar,
    address varchar
) with ('materialized' = 'true');
```

And the data we inserted manually - with a slight modification: we start IDs from 0 instead of 1, more on that later:

| ID | NAME                   | ADDRESS                |
|----|------------------------|------------------------|
| 0  | Gravitech Dynamics     | 222 Graviton Lane      |
| 1  | HyperDrive Innovations | 456 Warp Way           |
| 2  | DarkMatter Devices     | 333 Singularity Street |

A datagen connector that fills the table with this data is created with the following `curl` command:

```commandline
curl -L -X PUT 'http://localhost:8080/v0/connectors/vendor-datagen' \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d '{
  "config": {
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [
          { "limit": 3,
            "fields": {
              "name": { "values": ["Gravitech Dynamics", "HyperDrive Innovations", "DarkMatter Devices"] },
              "address": { "values": ["222 Graviton Lane", "456 Warp Way", "333 Singularity Street"] } } }
        ]
      }
    }
  },
  "description": "vendor datagen"
}'
```

First, we specify `datagen` as the transport. In the `config` section, we define a `plan` that describes how the
rows are generated. You can add multiple plans to this list, and they will be executed sequentially, but for now we only
need one.

In the plan we set the `limit` parameter, it specifies how many rows should be generated.
In `fields`, we describe how the values for each column should be generated: For the `name` and `address`
column, we give the list of the three names and addresses from the table above.
We don't need to configure anything for the `id` column because the default strategy for generating integer values is to
generate an incrementing sequence of numbers starting from 0.

We'll cover these "increment" generation strategies in more detail for the next table, `PART`:

```sql
create table PART (
    id bigint not null primary key,
    name varchar
);
```

This table was filled with the following content:

| ID | NAME           |
|----|----------------|
| 1  | Flux Capacitor |
| 2  | Warp Core      |
| 3  | Kyber Crystal  |

Let's define another connector for the `PART` table that generates this table. Here is the `curl` command:

```commandline
curl -L -X PUT 'http://localhost:8080/v0/connectors/part-datagen' \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d '{
  "config": {
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [
          { "limit": 3,
            "fields": {
              "id": { "strategy": increment", "range": [1, 4] },
              "name": { "values": ["Flux Capacitor", "Warp Core", "Kyber Crystal"] } } }
        ]
      }
    }
  },
  "description": "part datagen"
}'
```

Each member of the `fields` section can set a `strategy` that defines how the values are generated.
The `increment` strategy is the default, so we could've omitted it like in the previous table.
What's new is that we added the `range` parameter for the `id` column. That means we narrow the range of
values generated for this field. Instead of starting from 0 as we did in the previous table,
the `id` rows now have values `1, 2, 3`.

For the `name` column, we also (implicitly) use the `increment` strategy. Again, we specify a fixed set of
`values`. As seen previously, strategy will select the values from the list one-by-one.

The last table, `price` connects the vendor and part table:

```sql
create table PRICE (
  part bigint not null,
  vendor bigint not null,
  price decimal
) with ('materialized' = 'true');
```

As we did previously we'll insert the static content from Part 1 first, but then we add a second plan to the connector
that dynamically updates the prices and adds each part for every vendor to make it more interesting.
Here is the `curl` command:

```commandline
curl -L -X PUT 'http://localhost:8080/v0/connectors/price-datagen' \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d '{
  "config": {
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [
          { "limit": 3,
            "fields": {
              "part": { "range": [1, 4] },
              "vendor": { "values": [1, 0, 2] },
              "price": { "values": [10000, 15000, 9000] } } },
           { "rate": 1,
             "fields": {
              "vendor": { "values": [1, 0, 2] },
              "part": { "strategy": "zipf", "range": [1, 4] },
              "price": { "strategy": "uniform", "range": [9000, 15000] } }
           }
        ]
      }
    }
  },
  "description": "price datagen"
}'
```

The first plan similar to what we saw in the previous table (except we omit specifying the default increment
strategy). The second plan has some new settings. We add a `rate: 1` to tell the connector to emit one record
every second, and we omit `limit` so this plan will continuously generate records until the pipeline is stopped.
We keep the `vendor` column fixed, so every time we emit a record it will affect a different vendor.
Next we use a new strategy, a Zipf distribution for the `part` column. This means that the connector will
generate a random part ID, but the distribution of the IDs will be skewed towards the first ID in the range.
Finally, we set the `price` column to be generated with a `uniform` strategy, and a `range`, which means that the price
will be a random number between `9000` and `15000`.

## Step 2: Attach connectors to the pipeline

The last configuration step is to attach all our new connectors to our supply chain pipeline:

```commandline
curl -L -X PATCH 'http://localhost:8080/v0/pipelines/supply_chain' \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d '{
  "connectors": [
    {
      "connector_name": "vendor-datagen",
      "is_input": true,
      "name": "vendor-to-vendor-datagen",
      "relation_name": "vendor"
    },
    {
      "connector_name": "part-datagen",
      "is_input": true,
      "name": "part-to-part-datagen",
      "relation_name": "part"
    },
    {
      "connector_name": "price-datagen",
      "is_input": true,
      "name": "price-to-price-datagen",
      "relation_name": "price"
    }
  ],
  "name": "supply_chain",
  "description": "",
  "program_name": "supply_chain"
}'
```

## Step 3. Start and watch the pipeline

Finally, let's start the pipeline:

```commandline
curl -L -X POST 'http://localhost:8080/v0/pipelines/supply_chain/start' \
-H 'Content-Type: application/json' \
-H 'Accept: application/json'
```

You can go and inspect the pipeline and corresponding data in the WebConsole. You see a throughput of 1 record
per second with one row in prices changing every second.

To summarize Part 4 of the tutorial,

- We can attach a random generator to Feldera tables to simulate different scenarios such as backfill, continuous
  evaluation or a combination of the two.
  You'll find a complete reference for datagen in the [connectors section](../../connectors/sources/datagen).
- We got a glimpse of the HTTP API to configure a Feldera pipeline programmatically.
  You'll find more on this in [the REST API tutorial](tutorials/rest_api/index.md).
