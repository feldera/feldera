# HTTP output connector

Feldera supports receiving a stream of changes to a SQL table or view over HTTP.

* This output connector is ephemeral: it is created when the HTTP
  connection is opened, and is deleted once it is closed.

* It is the only output connector not created and managed by the user.

* Usage is through a special
  endpoint: [/v0/pipelines/:pipeline_name/egress/table_or_view_name?format=...](/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table)

* Specify data output format using URL query parameters
  (e.g., `format=...`, and more depending on format).

The HTTP output connector does not yet support [fault
tolerance](..#fault-tolerance).

## Example usage

We will subscribe to a stream of updates to the `average_price` view for pipeline `supply-chain-pipeline`.

### curl

```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/supply-chain-pipeline/egress/average_price?query=table\&mode=watch\&format=json
```

### Python (direct API calls)

```python
import requests

api_url = "http://localhost:8080"
headers = {"authorization": f"Bearer <API-KEY>"}

with requests.post(
        f'{api_url}/v0/pipelines/supply-chain-pipeline/egress/average_price?format=json',
        stream=True
) as f_in:
    for line in f_in:
        print(line.decode("utf-8").strip())
```

## Additional resources

For more information, see:

* [Tutorial section](/tutorials/basics/part2) on HTTP-based input and output.

* [REST API documentation](/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table)
  for the `/egress` endpoint.
