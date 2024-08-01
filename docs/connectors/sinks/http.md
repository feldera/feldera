# HTTP output connector

Feldera supports receiving a stream of changes to a SQL table or view over HTTP.

* This output connector is ephemeral: it is created when the HTTP
  connection is opened, and is deleted once it is closed.

* It is the only output connector not created and managed by the user.

* Usage is through a special endpoint:

  **[/v0/pipelines/:pipeline_name/egress/:table_or_view_name?format=...](https://www.feldera.com/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table)**

* Specify data output format using URL query parameters
  (e.g., `format=...`, and more depending on format).

* Specify egress query (table, neighborhood, quantiles) and mode (watch, snapshot)
  using URL query parameters (e.g., `query=quantiles&mode=snapshot`).

* The `query=quantiles` mode is in fact a paginated query that returns
  a limited number of rows from the queried view.  An additional
  parameter `quantiles=q` can be used to specify in 1/10s of a percentage
  the starting point of the page.

* The "snapshot" mode can be applied to tables and views only if they
  are declared as MATERIALIZED in SQL.

## Example usage

We will check the content of table `product` or view `average_price` for pipeline `supply-chain-pipeline`.

### curl

#### Watch changes
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/egress/average_price?query=table\&mode=watch\&format=json
```

#### Snapshot quantiles
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/egress/product?query=quantiles\&mode=snapshot\&format=json
```

#### Get a snapshot around the median value in table `product` (quantiles=500)
```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/egress/product?query=quantiles\&mode=snapshot\&format=json\&quantiles=500
```

### Python (direct API calls)

#### Watch changes
```python
import requests

api_url = "http://localhost:8080"
headers = { "authorization": f"Bearer <API-KEY>" }

with requests.post(
    f'{api_url}/v0/pipelines/supply-chain-pipeline/egress/average_price?query=table&mode=watch&format=json',
    stream=True
) as f_in:
    for line in f_in:
        print(line.decode("utf-8").strip())
```

#### Snapshot quantiles
```python
import json
import requests

api_url = "http://localhost:8080"
headers = { "authorization": f"Bearer <API-KEY>" }

response = requests.post(
    f'{api_url}/v0/pipelines/supply-chain-pipeline/egress/product?query=quantiles&mode=snapshot&format=json'
).content
print(json.dumps(json.loads(response)['json_data'], indent=4))
```

## Additional resources

For more information, see:

* [Tutorial section](/docs/tutorials/basics/part2) on HTTP-based input and output.

* [REST API documentation](https://www.feldera.com/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table)
  for the `/egress` endpoint.
