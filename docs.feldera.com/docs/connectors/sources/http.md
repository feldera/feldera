# HTTP input connector

Feldera supports directly pushing data to a SQL table over HTTP.

* Unlike other input connectors that must be created by the user as part of the SQL table
  declaration, the HTTP input connector is created automatically for each table in the pipeline.

* Usage is through a special
  endpoint: [/v0/pipelines/:pipeline_name/ingress/:table_name?format=...](/api/insert-data)

* Specify data input format using URL query parameters
  (e.g., `format=...`, and more depending on format).

The HTTP input connector supports [fault
tolerance](/pipelines/fault-tolerance).

## Example usage

We will insert rows into table `product` for pipeline `supply-chain-pipeline`.

### curl

#### One row

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json' \
  -d '{"insert": {"pid": 0, "name": "hammer", "price": 5.0}}'
```

#### One row while providing authorization header

```bash
curl -i -H "Authorization: Bearer <API-KEY>" -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json' \
  -d '{"insert": {"pid": 0, "name": "hammer", "price": 5.0}}'
```

#### Multiple rows as newline-delimited JSON (NDJSON)

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json' \
  -d '{"insert": {"pid": 0, "name": "hammer", "price": 5}}
{"insert": {"pid": 1, "name": "nail", "price": 0.02}}'
```

#### Multiple rows as a JSON array (note: URL parameter `array=true`)

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json&array=true' \
  -d '[{"insert": {"pid": 0, "name": "hammer", "price": 5}}, {"insert": {"pid": 1, "name": "nail", "price": 0.02}}]'
```

#### Delete a row

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json' \
  -d '{"delete": {"pid": 1}}'
```

### Python (direct API calls)

#### Insert 1000 rows in batches of 50

Insert 1000 products named "hammer" with unique product identifiers
and a random price between 1 and 100. Batching can improve throughput.

```python
import random
import requests

api_url = "http://127.0.0.1:8080"
headers = {"authorization": f"Bearer <API-KEY>"}

batch = []
for product_id in range(0, 1000):
    batch.append({"insert": {
        "pid": product_id, "name": "hammer", "price": random.uniform(1.0, 100.0)
    }})
    if len(batch) >= 50 or product_id == 999:
        requests.post(
            f"{api_url}/v0/pipelines/supply-chain-pipeline/ingress/product?format=json&array=true",
            json=batch, headers=headers
        ).raise_for_status()
        batch.clear()
```

### Python (using Python API)

#### Insert 1000 rows in batches of 50

Insert 1000 products named "hammer" with unique product identifiers
and a random price between 1 and 100. Batching can improve throughput.

```python
import random
import requests
from feldera import FelderaClient

api_key = "<API-KEY>"
CLIENT = FelderaClient("http://127.0.0.1:8080", api_key)

batch = []
for product_id in range(0, 1000):
    batch.append({"insert": {
        "pid": product_id, "name": "hammer", "price": random.uniform(1.0, 100.0)
    }})
    if len(batch) >= 50 or product_id == 999:
        CLIENT.push_to_pipeline(
            pipeline_name="supply-chain-pipeline",
            table_name="product",
            format="json",
            array=true,
            data=batch)
        batch.clear()
```

## Connector metadata

A request can attach "connector metadata" to the records it carries.  Pass the metadata as a JSON object in the `connector_metadata` query parameter.  The
[`CONNECTOR_METADATA()`](/sql/grammar#connector_metadata) function returns
the object for every record of the request, so a column declared with
`DEFAULT CAST(CONNECTOR_METADATA()['name'] AS type)` receives a value from the
`name` attribute of the object.  The parameter works with every input format.

For example, a test can feed a table declared for a Kafka connector through
the HTTP connector:

```sql
CREATE TABLE events (
  id BIGINT,
  kafka_topic VARCHAR DEFAULT CAST(CONNECTOR_METADATA()['kafka_topic'] AS VARCHAR),
  kafka_offset BIGINT DEFAULT CAST(CONNECTOR_METADATA()['kafka_offset'] AS BIGINT)
);
```

The following request inserts the row `(1, 'orders', 42)`.  The
`connector_metadata` value is the URL encoding of
`{"kafka_topic": "orders", "kafka_offset": 42}`:

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/ingress/events?format=json&update_format=raw&connector_metadata=%7B%22kafka_topic%22%3A%22orders%22%2C%22kafka_offset%22%3A42%7D' \
  -d '{"id": 1}'
```

The Python API takes the metadata as a dictionary:

```python
pipeline.input_json(
    "events",
    [{"id": 1}],
    connector_metadata={"kafka_topic": "orders", "kafka_offset": 42},
)
```

A request without the parameter inserts records for which
`CONNECTOR_METADATA()` returns `NULL`, so the metadata columns take the
`NULL` default.  A `connector_metadata` value that is not a JSON object is
rejected with status 400 before the record is ingested.

## Additional resources

For more information, see:

* [Tutorial section](/tutorials/basics/part2) on HTTP-based input and output.

* [REST API documentation](/api/insert-data) for the `/ingress` endpoint.

* Data formats such as [JSON](/formats/json) and
  [CSV](/formats/csv)

* [Python API documentation](pathname:///python/)
