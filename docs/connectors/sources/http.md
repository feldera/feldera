# HTTP input connector

Feldera supports directly pushing data to a SQL table over HTTP.

* This input connector is ephemeral: it is created when the HTTP
  connection is opened, and is deleted once it is closed.

* It is the only input connector not created and managed by the user.

* Usage is through a special endpoint:

  **[/v0/pipelines/:pipeline_name/ingress/:table_name?format=...](https://www.feldera.com/api/push-data-to-a-sql-table)**

* Specify data input format using URL query parameters
  (e.g., `format=...`, and more depending on format).

## Example usage

We will insert rows into table `product` for pipeline `supply-chain-pipeline`.

### curl

#### One row
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json \
  -d '{"insert": {"pid": 0, "name": "hammer", "price": 5.0}}'
```

#### One row while providing authorization header
```bash
curl -i -H "Authorization: Bearer <API-KEY>" -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json \
  -d '{"insert": {"pid": 0, "name": "hammer", "price": 5.0}}'
```

#### Multiple rows as newline-delimited JSON (NDJSON)
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json \
  -d '{"insert": {"pid": 0, "name": "hammer", "price": 5}}
{"insert": {"pid": 1, "name": "nail", "price": 0.02}}'
```

#### Multiple rows as a JSON array (note: URL parameter `array=true`)
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json\&array=true \
  -d '[{"insert": {"pid": 0, "name": "hammer", "price": 5}}, {"insert": {"pid": 1, "name": "nail", "price": 0.02}}]'
```

#### Delete a row
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/ingress/product?format=json \
  -d '{"delete": {"pid": 1}}'
```

### Python (direct API calls)

#### Insert 1000 rows in batches of 50

Insert 1000 products named "hammer" with unique product identifiers
and a random price between 1 and 100. Batching can improve throughput.

```python
import random
import requests

api_url = "http://localhost:8080"
headers = { "authorization": f"Bearer <API-KEY>" }

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

## Additional resources

For more information, see:

* [Tutorial section](/docs/tutorials/basics/part2) on HTTP-based input and output.

* [REST API documentation](https://www.feldera.com/api/push-data-to-a-sql-table) for the `/ingress` endpoint.

* Data formats such as [JSON](https://www.feldera.com/docs/api/json) and
  [CSV](https://www.feldera.com/docs/api/csv)
