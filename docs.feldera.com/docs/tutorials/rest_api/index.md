# Using the REST API

Feldera features a comprehensive REST API for managing
[pipelines](/api/introduction#pipeline).
In fact, Feldera's Web Console interacts with the backend service exclusively
via this public API.

In this tutorial we will focus on invoking the
API endpoints directly via `curl`. Once you become familiar with the API,
these calls can be automated in your favorite scripting or programming
language (e.g., in Python using the `requests` module).

## Setup

1. **curl:** You must have **curl** installed.

   > Throughout this tutorial we will use several curl options:
   > - `-i` shows the response headers
   > - `-X <METHOD>` specifies the request method (e.g., GET, PUT, POST, PATCH, DELETE, ...)
   > - `-N` disables output buffering
   > - `-H <HEADER>` specifies a request header
   > - `-s` enables silent mode

2. **jq:** We'll use this for some JSON manipulation from your terminal.

3. **Feldera instance:**  If you haven't done so already, you can start Feldera locally using
   [**docker**](https://docs.docker.com/engine/install/):
   ```
   curl -L https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml | \
   docker compose -f - up
   ```
   (leave it running in a separate terminal while going through this tutorial)

   > For the remainder of this tutorial, we will use http://127.0.0.1:8080 as
   > this is the default local hostname:port for the docker Feldera instance.
   > You will need to change it to match the Feldera instance you are using.

4. **(Optional) API key:** If you're using Feldera via our public sandbox or enterprise offering,
   your instance will requires authentication. If so, login to your Feldera instance and
   and generate an API key in the Web Console via the _User Profile_ icon on the top right of the UI.
   You can add it to a `curl` call by replacing `<API-KEY>`
   with the generated string starting with `apikey:...`:
   ```
   curl -s -H "Authorization: Bearer <API-KEY>" -X GET http://127.0.0.1:8080/v0/pipelines | jq
   ```

   > For the remainder of this tutorial, you will need to add
   > `-H "Authorization: Bearer <API-KEY>"` to each of the calls.

5. **Check whether your setup works:** You can verify your setup by running:
   ```
   curl -s -X GET http://127.0.0.1:8080/v0/programs | jq
   ```

   ... this will output a JSON array of program objects, which when there are
   none (yet!) is empty:

   ```
   []
   ```

**Congratulations, you've already interacted with the REST API!**

## Getting started

In this tutorial, we are going to approach a use case of supply chain management,
which we also looked at in the
[UI-based tutorial](/tutorials/basics/).
The use case focuses on identifying the vendors with the lowest prices for parts.
We will create a pipeline which ingests data from several HTTP sources
and performs several interesting queries on them.

We'll be going through the following steps:

1. Create and compile a pipeline with data connectors
2. Start the pipeline
3. Check pipeline progress
4. Read data directly from a view
5. Feed data directly into a table
6. Cleanup

... all using just `curl`!

> Note: at any point in the tutorial, don't forget you can check out the
> Web Console by visiting http://127.0.0.1:8080 in your browser!

### Step 1: SQL pipeline

The SQL pipeline defines tables using `CREATE TABLE` statements
and views using `CREATE VIEW` statements. We will create three tables,
namely `vendor`, `part`, and `price`. We will connect each table to different data sources using
our URL connector. For views, we'll create `low_price` which lists
the lowest available price for each part across all vendors, and `preferred_vendor` which
supplements the lowest price by adding vendor information.

Create a file called **program.sql** with the following content:

```
CREATE TABLE vendor (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR,
    address VARCHAR
)  WITH ('connectors' = '[{
    "transport": {
        "name": "url_input", "config": {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json"}
    },
    "format": { "name": "json" }
}]');

CREATE TABLE part (
    id bigint NOT NULL PRIMARY KEY,
    name VARCHAR
) WITH ('connectors' = '[{
    "transport": {
        "name": "url_input", "config": {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"  }
    },
    "format": { "name": "json" }
}]');


CREATE TABLE price (
    part BIGINT NOT NULL,
    vendor BIGINT NOT NULL,
    price DECIMAL
)
WITH ('connectors' = '[{
    "transport": {
        "name": "url_input", "config": {"path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"  }
    },
    "format": { "name": "json" }
}]');

CREATE VIEW low_price
    (part, price)
    AS
    SELECT
        price.part AS part,
        MIN(price.price) AS price
    FROM price
    GROUP BY price.part;

CREATE MATERIALIZED VIEW preferred_vendor
    (part_id, part_name, vendor_id, vendor_name, price)
    AS
    SELECT
        part.id AS part_id,
        part.name AS part_name,
        vendor.id AS vendor_id,
        vendor.name AS vendor_name,
        price.price AS price
    FROM
        price, part, vendor, low_price
    WHERE
        price.price = low_price.price AND
        price.part = low_price.part AND
        part.id = price.part AND
        vendor.id = price.vendor;
```

Next, let's create a pipeline out of the above program. We'll use `jq` to create a JSON object that
specifies the pipeline's name, description, the different configuration paramters, and fill in the
contents of `program.sql` into the `program_code` field.

```
curl -i -X PUT http://127.0.0.1:8080/v0/pipelines/supply-chain \
-H 'Content-Type: application/json' \
-d "$(jq -Rsn \
  --rawfile code program.sql \
  '{
    name: "supply-chain",
    description: "Supply Chain Tutorial",
    runtime_config: {
      workers: 4
    },
    program_config: {},
    program_code: $code
  }')"
```

As response, we should get back `HTTP/1.1 201 Created` along with the identifier of
the program and its version (1). When an SQL program is created or when its code is
updated, its version is incremented and compilation is automatically triggered.

Now let's check the program's compilation status a few times:

```
curl -s http://127.0.0.1:8080/v0/pipelines/supply-chain | jq '.program_status'
```
...which will show "CompilingRust" at first, but in about 30 seconds or so say "Success".

The pipeline is now ready to be started.

### Step 2: Starting pipeline

We start the pipeline using:

```
curl -i -X POST http://127.0.0.1:8080/v0/pipelines/supply-chain/start
```

... which will return `HTTP/1.1 202 Accepted` when successful.

Check that it has successfully started using:

```
curl -s GET http://127.0.0.1:8080/v0/pipelines/supply-chain | jq '.deployment_status'
```

... which will say 'Running` when the pipeline has started:

> Note: Connectors are only initialized when a pipeline starts to use them.
> A pipeline will not start if a connector is unable to connect to its
> data source or sink (e.g., if a URL is misspelled).

> To restart a pipeline (e.g., to have updates to its program or schema
> take effect):
> ```
> # Shut it down:
> curl -i -X POST http://127.0.0.1:8080/v0/pipelines/supply-chain/shutdown
> # ... wait for the current_status to become Shutdown by checking:
> curl -X GET http://127.0.0.1:8080/v0/pipelines/supply-chain
> # ... and then start:
> curl -i -X POST http://127.0.0.1:8080/v0/pipelines/supply-chain/start
> ```

### Step 3: Pipeline progress

A running pipeline provides several useful stats:

```
curl -sX GET http://127.0.0.1:8080/v0/pipelines/supply-chain/stats | jq
```

... such as the number of input and processed records.
The `total_processed_records` should be `9` (3 rows each for part, vendor, and price).
An example output:

```
{
  ...
  "global_metrics": {
    ...
    "total_input_records": 9,
    "total_processed_records": 9,
    ...
  },
  ...
}
```


### Step 4: Read data directly from a view

Both input and output connectors are optional, in the sense that input and
output of data can directly be performed using HTTP requests as well.

We can retrieve a snapshot of the `preferred\_vendor` view using `curl`:

```
curl -X POST 'http://127.0.0.1:8080/v0/pipelines/supply-chain/egress/PREFERRED_VENDOR?format=json&mode=snapshot&query=quantiles' | jq
```

... which for each of the parts will show the preferred vendor:

```
{
  "sequence_number": 0,
  "json_data": [
    {
      "insert": {
        "PART_ID": 1,
        "PART_NAME": "Flux Capacitor",
        "VENDOR_ID": 2,
        "VENDOR_NAME": "HyperDrive Innovations",
        "PRICE": "10000"
      }
    },
    {
      "insert": {
        "PART_ID": 2,
        "PART_NAME": "Warp Core",
        "VENDOR_ID": 1,
        "VENDOR_NAME": "Gravitech Dynamics",
        "PRICE": "15000"
      }
    },
    {
      "insert": {
        "PART_ID": 3,
        "PART_NAME": "Kyber Crystal",
        "VENDOR_ID": 3,
        "VENDOR_NAME": "DarkMatter Devices",
        "PRICE": "9000"
      }
    }
  ]
}
```

This includes Gravitech Dynamics (vendor id: 1) for the Warp Core (part id: 2)
at a price point of 15000.

It is also possible to actively monitor a view for changes rather than
retrieving a snapshot:

```
curl -s -N -X POST 'http://127.0.0.1:8080/v0/pipelines/supply-chain/egress/PREFERRED_VENDOR?format=json&mode=watch' | jq
```

Keep this open in a separate terminal for the next step.
Even if there is no changes it will regularly send an empty message.

### Step 5: Feed data directly into a table

It is possible to INSERT, UPSERT or even DELETE a single row within a table. In this case,
we have HyperDrive Innovations supply the Warp Core at a lower price of 12000:

```
curl -X 'POST' http://127.0.0.1:8080/v0/pipelines/supply-chain/ingress/PRICE?format=json \
-d '{"insert": {"part": 2, "vendor": 2, "price": 12000}}'
```

In the other terminal, we can see the preferred vendor view output change,
with a row deletion (with the previous cheapest vendor) and insertion
(with the new cheapest vendor):

```
...
{
  "sequence_number": 2,
  "json_data": [
    {
      "insert": {
        "PART_ID": 2,
        "PART_NAME": "Warp Core",
        "VENDOR_ID": 2,
        "VENDOR_NAME": "HyperDrive Innovations",
        "PRICE": "12000"
      }
    },
    {
      "delete": {
        "PART_ID": 2,
        "PART_NAME": "Warp Core",
        "VENDOR_ID": 1,
        "VENDOR_NAME": "Gravitech Dynamics",
        "PRICE": "15000"
      }
    }
  ]
}
...
```

### Step 6: Cleanup

After you are done with the tutorial, we can clean up. First, shut
down the pipeline (which will automatically terminate monitoring the
view if it is still running):

```
curl -i -X POST http://127.0.0.1:8080/v0/pipelines/supply-chain/shutdown
```

Check that it has been shut down using:

```
curl -s http://127.0.0.1:8080/v0/pipelines/supply-chain | jq '.deployment_status'
```
... and you  should see `deployment_status` set to `Shutdown`.

Next, let's DELETE the pipeline:
```
curl -i -X DELETE http://127.0.0.1:8080/v0/pipelines/supply-chain
```

You can also delete the `program.sql` file we used to create the program.

If you are using the docker test setup, you can stop the Feldera docker instance
using Ctrl-C.

## Next steps

Interested in building applications using the API? Consider reading our API and SQL reference.

- [Browse the API documentation](/api/)
- [Read the SQL reference](/sql/intro/)
