# HTTP GET (URL) input connector

Feldera can ingest data from a user-provided URL into a SQL table.

* The file is fetched using HTTP with the GET method.

## Example usage

We will create an HTTP GET connector named `product-tools`.

The file is hosted at `https://example.com/tools-data.json`,
and is in [newline-delimited JSON (NDJSON) format](/docs/api/json#encoding-multiple-changes)
with one row per line. For example:

```text
{"insert": {"pid": 0, "name": "hammer", "price": 5}}
{"insert": {"pid": 1, "name": "nail", "price": 0.02}}
{"delete": {"pid": 0}}
```

### curl

```bash
curl -i -X PUT http://localhost:8080/v0/connectors/product-tools \
-H "Authorization: Bearer <API-KEY>" \
-H 'Content-Type: application/json' \
-d '{
  "description": "URL input connector for tools products",
  "config": {
      "transport": {
          "name": "url_input",
          "config": {
              "path": "https://example.com/tools-data.json"
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
}'
```

### Python (direct API calls)

```python
import requests

api_url = "http://localhost:8080"
headers = { "authorization": f"Bearer <API-KEY>" }

requests.put(
    f"{api_url}/v0/connectors/product-tools", 
    headers=headers,
    json={
        "description": "URL input connector for tools products",
        "config": {
            "transport": {
                "name": "url_input",
                "config": {
                    "path": "https://example.com/tools-data.json"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    "array": False
                }
            }
        }
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
