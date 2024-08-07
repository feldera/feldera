# HTTP GET (URL) input connector

Feldera can ingest data from a user-provided URL into a SQL table.

* The file is fetched using HTTP with the GET method.

## Example usage

We will create a pipeline with an HTTP GET connector.

The file is hosted at `https://example.com/tools-data.json`,
and is in [newline-delimited JSON (NDJSON) format](/docs/api/json#encoding-multiple-changes)
with one row per line. For example:

```text
{"insert": {"pid": 0, "name": "hammer", "price": 5}}
{"insert": {"pid": 1, "name": "nail", "price": 0.02}}
{"delete": {"pid": 0}}
```

### SQL example file

Create a file named **program.sql** with the following content:
```
CREATE TABLE price (
    pid BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    price DECIMAL
)
WITH ('connectors' = '[{
    "transport": {
        "name": "url_input",
        "config": {"path": "https://example.com/tools-data.json"}
    },
    "format": {
        "name": "json",
        "config": {
            "update_format": "insert_delete",
            "array": false
        }
    }
}]');
```

### curl

```bash
curl -i -X PUT http://localhost:8080/v0/pipelines/workshop \
-H 'Content-Type: application/json' \
-d "$(jq -Rsn \
  --rawfile code program.sql \
  '{
    name: "workshop",
    description: "Workshop inventory",
    runtime_config: {
      workers: 4
    },
    program_config: {},
    program_code: $code
  }')"
```

### Python (direct API calls)

```python
import requests

api_url = "http://localhost:8080"
headers = { "authorization": f"Bearer <API-KEY>" }
requests.put(
    f"{api_url}/v0/pipelines/workshop", 
    headers=headers,
    json={
      "name": "workshop",
      "description": "Workshop inventory",
      "runtime_config": {
        "workers": 4
      },
      "program_config": {},
      "program_code": open("program.sql").read()
    }
).raise_for_status()
```

## Additional resources

For more information, see:

* [API connectors documentation](https://www.feldera.com/api/create-a-new-connector)

* [Tutorial section](/docs/tutorials/basics/part3#step-1-create-http-get-connectors) which involves
  creating an HTTP GET connector.

* Data formats such as [JSON](https://www.feldera.com/docs/api/json),
  [CSV](https://www.feldera.com/docs/api/csv), and [Parquet](https://www.feldera.com/docs/api/parquet)
