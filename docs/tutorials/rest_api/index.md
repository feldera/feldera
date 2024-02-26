# Using REST API

:::caution

The REST API is still evolving and might see backwards incompatible changes.

:::

The Feldera platform features a comprehensive REST API for managing
[programs](https://www.feldera.com/docs/#programs),
[connectors](https://www.feldera.com/docs/#connectors), 
and [pipelines](https://www.feldera.com/docs/#pipelines).
Feldera's Web Console UI interacts with the backend service exclusively
via this public API; hence all functionality available in the Web Console
is [available via the API](https://www.feldera.com/api/).
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

2. **(Optional) jq:** It is recommended to have **jq** installed as it improves
   the readability of the JSON output of the API.

   > The usage of `jq` is optional; to not use, remove the `| jq` at the end of some
   > of the `curl` calls .

3. **Feldera instance:** This tutorial requires you to have a running Feldera instance 
   to interact with. If you do not have one already, you can start one locally using
   [**docker**](https://docs.docker.com/engine/install/):
   ```
   curl https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml | \
   docker compose -f - up
   ```
   (leave it running in a separate terminal while going through this tutorial)

   > For the remainder of this tutorial, we will use **http://localhost:8080** as
   > this is the default local hostname:port for the docker Feldera instance.
   > You will need to change it to match the Feldera instance you are using.

4. **(Optional) API key:** Skip this step if you are using a local test setup
   using Docker as described above. If the Feldera instance requires authentication,
   you must generate an API key in the Web Console at the _Settings_ tab.
   You can add it to a `curl` call in the following way:
   ```
   curl -s -H "Authorization: <API-KEY>" -X GET http://localhost:8080/v0/programs | jq
   ```

   > For the remainder of this tutorial, you will need to add
   > `-H "Authorization: <API-KEY>"` to each of the calls.

5. **Check it's working:** You can verify it's working by running:
   ```
   curl -s -X GET http://localhost:8080/v0/programs | jq
   ```
   
   ... this will output a JSON array of program objects, which when there are
   none (yet!) is empty:

   ```
   []
   ```

**Congratulations, you've already done your first direct API interaction!**

## Getting started

In this tutorial, we are going to approach a use case of supply chain management,
which we also looked at in the
[UI-based tutorial](https://www.feldera.com/docs/tutorials/basics/).
The use case focuses on identifying the vendors with the lowest prices for parts.
We will create a pipeline which ingests data from several HTTP sources
and performs several interesting queries on them.

We'll be going through the following steps:

1. Create and compile an SQL program
2. Create data connectors
3. Create a pipeline with the prepared program and connectors
4. Start the pipeline
5. Check pipeline progress
6. Read data directly from a view
7. Feed data directly into a table
8. Cleanup

... all using just `curl`!

> Note: at any point in the tutorial, don't forget you can check out the
> Web Console by visiting **http://localhost:8080** in your browser!

### Step 1: SQL program

The SQL program defines the tables using `CREATE TABLE` statements
and views using `CREATE VIEW` statements. We will create three tables,
namely `vendor`, `part`, and `price`. As views, we'll create `low_price` which lists
the lowest available price for each part across all vendors, and `preferred_vendor` which
supplements the lowest price by adding vendor information.

Because the SQL code is multiple lines and we wish to preserve its readability,
for this `curl` request only we will input the JSON data by file rather than
specifying in-line.

Create a file called **program.json** with the following content:

```
{
    "description": "Supply Chain program",
    "code": "
CREATE TABLE vendor (                                    \n
    id BIGINT NOT NULL PRIMARY KEY,                      \n
    name VARCHAR,                                        \n
    address VARCHAR                                      \n
);                                                       \n
                                                         \n
CREATE TABLE part (                                      \n
    id bigint NOT NULL PRIMARY KEY,                      \n
    name VARCHAR                                         \n
);                                                       \n
                                                         \n
CREATE TABLE price (                                     \n
    part BIGINT NOT NULL,                                \n
    vendor BIGINT NOT NULL,                              \n
    price DECIMAL                                        \n
);                                                       \n
                                                         \n
CREATE VIEW low_price                                    \n
    (part, price)                                        \n
    AS                                                   \n
    SELECT                                               \n
        price.part AS part,                              \n
        MIN(price.price) AS price                        \n
    FROM price                                           \n
    GROUP BY price.part;                                 \n
                                                         \n
CREATE VIEW preferred_vendor                             \n
    (part_id, part_name, vendor_id, vendor_name, price)  \n
    AS                                                   \n
    SELECT                                               \n
        part.id AS part_id,                              \n
        part.name AS part_name,                          \n
        vendor.id AS vendor_id,                          \n
        vendor.name AS vendor_name,                      \n
        price.price AS price                             \n
    FROM                                                 \n
        price, part, vendor, low_price                   \n
    WHERE                                                \n
        price.price = low_price.price AND                \n
        price.part = low_price.part AND                  \n
        part.id = price.part AND                         \n
        vendor.id = price.vendor;                        \n
"
}
```

... and subsequently create the program named `sc-program` by running
in the same directory:

```
curl -i -X PUT http://localhost:8080/v0/programs/sc-program \
-H 'Content-Type: application/json' \
-d @program.json
```

As response, we should get back `HTTP/1.1 201 Created` along with the identifier of
the program and its version (1). When an SQL program is created or when its code is
updated, its version is incremented and compilation is automatically triggered.

Check the program's compilation status:

```
curl -s -X GET http://localhost:8080/v0/programs/sc-program | jq
```

... which should contain in its output if the program's Rust code is being compiled:

```
{
  ...
  "name": "sc-program",
  "description": "Supply Chain program",
  "version": 1,
  "status": "CompilingRust",
  "schema": {
    ...
  },
  "code": null
}
```

... and when the program is compiled:

```
{
  ...
  "status": "Success",
  ...
}
```

> The program status will change from `Pending`, to `CompilingSql`, to `CompilingRust`,
> and finally to `Success`. There are in addition statuses which indicate errors if
> compilation fails: `SqlError`, `RustError`, and `SystemError`.

### Step 2: Data connectors

Data connectors are used to both input data sources into tables,
and output data from views to data sinks. In this tutorial,
we will define an HTTP source for each of the tables:

* **Part (named `sc-connector-part`):**
  ```
  curl -i -X PUT http://localhost:8080/v0/connectors/sc-connector-part \
  -H 'Content-Type: application/json' \
  -d '{
    "description": "Connector for part",
    "config": {
        "transport": {
            "name": "url",
            "config": {
                "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
            }
        },
        "format": {
            "name": "json",
            "config": {}
        }
    }
  }'
  ```

* **Vendor (named `sc-connector-vendor`):**
  ```
  curl -i -X PUT http://localhost:8080/v0/connectors/sc-connector-vendor \
  -H 'Content-Type: application/json' \
  -d '{
    "description": "Connector for vendor",
    "config": {
        "transport": {
            "name": "url",
            "config": {
                "path": "https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json"
            }
        },
        "format": {
            "name": "json",
            "config": {}
        }
    }
  }'
  ```

* **Price (named `sc-connector-price`):**
  ```
  curl -i -X PUT http://localhost:8080/v0/connectors/sc-connector-price \
  -H 'Content-Type: application/json' \
  -d '{
    "description": "Connector for price",
    "config": {
        "transport": {
            "name": "url",
            "config": {
                "path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"
            }
        },
        "format": {
            "name": "json",
            "config": {}
        }
    }
  }'
  ```

Each of the above should execute almost instantly and return `HTTP/1.1
201 Created`.

We can view the existing connectors using:

```
curl -s -X GET http://localhost:8080/v0/connectors | jq
```

... which will return:

```
[
  {
    ...
    "name": "sc-connector-vendor",
    "description": "Connector for vendor",
    "config": {
      ...
    }
  },
  {
    ...
    "name": "sc-connector-part",
    "description": "Connector for part",
    "config": {
      ...
    }
  },
  {
    ...
    "name": "sc-connector-price",
    "description": "Connector for price",
    "config": {
      ...
  }
]
```

> In this tutorial we do not create output connectors. We will read the
> views via the API (see step 6). The procedure to create output connectors
> is the same as for input connectors, albeit of course with its own
> specific configuration (e.g., providing output topic in a Kafka output
> connector config).

### Step 3: Pipeline creation

We create the pipeline referring by name to the program and all
the connectors we have created previously. Each connector is
attached as input to their respective table.

```
curl -i -X PUT http://localhost:8080/v0/pipelines/sc-pipeline \
-H 'Content-Type: application/json' \
-d '{
    "description": "Supply Chain pipeline",
    "program_name": "sc-program",
    "config": {"workers": 4},
    "connectors": [
         {
             "connector_name": "sc-connector-part",
             "is_input": true,
             "name": "sc-connector-part",
             "relation_name": "PART"
         },
         {
             "connector_name": "sc-connector-vendor",
             "is_input": true,
             "name": "sc-connector-vendor",
             "relation_name": "VENDOR"
         },
         {
             "connector_name": "sc-connector-price",
             "is_input": true,
             "name": "sc-connector-price",
             "relation_name": "PRICE"
         }
    ]
}'
```

> Observe in the above format that it is possible to connect multiple
> connectors to the same table or view.

It will return `HTTP/1.1 201 Created` quickly, indicating success.
We can retrieve the pipeline using:

```
curl -X GET http://localhost:8080/v0/pipelines/sc-pipeline | jq
```

... which will output:

```
{
  "descriptor": {
    ...
    "program_name": "sc-program",
    "version": 1,
    "name": "sc-pipeline",
    "description": "Supply Chain pipeline",
    "config": {
      ...
    },
    "attached_connectors": [
      ...
    ]
  },
  "state": {
    ...
    "current_status": "Shutdown",
    ...
  }
}
```

The pipeline is now ready to be started.

### Step 4: Starting pipeline

We start the pipeline using:

```
curl -i -X POST http://localhost:8080/v0/pipelines/sc-pipeline/start
```

... which will return `HTTP/1.1 202 Accepted` when successful.

Check it is successfully started using:

```
curl -X GET http://localhost:8080/v0/pipelines/sc-pipeline | jq
```

... which will have `current_status` set to `Running` when it has started:

```
{
  ...
  },
  "state": {
    ...
    "current_status": "Running",
    ...
  }
  ...
}
```


> Note: Connectors are only initialized when a pipeline starts to use them.
> A pipeline will not start if a connector is unable to connect to its
> data source or sink (e.g., if a URL is misspelled).

> To restart a pipeline (e.g., to have updates to its program or schema
> take effect):
> ```
> # Shut it down:
> curl -i -X POST http://localhost:8080/v0/pipelines/sc-pipeline/shutdown
> # ... wait for the current_status to become Shutdown by checking:
> curl -X GET http://localhost:8080/v0/pipelines/sc-pipeline
> # ... and then start:
> curl -i -X POST http://localhost:8080/v0/pipelines/sc-pipeline/start
> ```

### Step 5: Pipeline progress

A running pipeline provides a multitude of interesting stats:

```
curl -X GET http://localhost:8080/v0/pipelines/sc-pipeline/stats | jq
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


### Step 6: Read data directly from a view

Both input and output connectors are optional, in the sense that input and
output of data can directly be done using HTTP requests as well.

We can retrieve the view quantiles snapshot using `curl`:

```
curl -X POST 'http://localhost:8080/v0/pipelines/sc-pipeline/egress/PREFERRED_VENDOR?format=json&mode=snapshot&query=quantiles' | jq
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
curl -s -N -X POST 'http://localhost:8080/v0/pipelines/sc-pipeline/egress/PREFERRED_VENDOR?format=json&mode=watch' | jq
```

Keep this open in a separate terminal for the next step.
Even if there is no changes it will regularly send an empty message.

### Step 7: Feed data directly into a table

It is possible to INSERT, UPSERT or even DELETE a single row within a table. In this case,
we have HyperDrive Innovations supply the Warp Core at a lower price of 12000:

```
curl -X 'POST' http://localhost:8080/v0/pipelines/sc-pipeline/ingress/PRICE?format=json \
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

### Step 8: Cleanup

After you are done with the tutorial, we can clean up. First, shut
down the pipeline (which will automatically terminate monitoring the
view if it is still running):

```
curl -i -X POST http://localhost:8080/v0/pipelines/sc-pipeline/shutdown
```

Check it is shut down using:

```
curl -i -X GET http://localhost:8080/v0/pipelines/sc-pipeline
```
... which should have `current_status` set to `Shutdown`.

Next, all resources can be deleted in reverse order of creation:
```
curl -i -X DELETE http://localhost:8080/v0/pipelines/sc-pipeline
curl -i -X DELETE http://localhost:8080/v0/connectors/sc-connector-part
curl -i -X DELETE http://localhost:8080/v0/connectors/sc-connector-vendor
curl -i -X DELETE http://localhost:8080/v0/connectors/sc-connector-price
curl -i -X DELETE http://localhost:8080/v0/programs/sc-program
```

You can also delete the `program.json` file we used to create the program.

If you are using the docker test setup, you can stop the Feldera docker instance
using Ctrl-C. This should shut down the containers. This generally works,
although sometimes docker fails to shut down the containers and leaves
them running in the background. In this case, you can also explicitly
shut them down using `docker compose down`:

```
curl https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml | \
docker compose -f - down
```

## Next steps

We hope you found this interesting.
Please consider reading additional information at:

- [Browse the API documentation](https://www.feldera.com/api/)
- [Read the SQL reference](https://www.feldera.com/docs/sql/intro/)
