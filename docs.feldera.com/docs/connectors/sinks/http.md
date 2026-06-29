# HTTP output connector

Feldera supports receiving a stream of changes to a SQL table or view over HTTP.

* This output connector is ephemeral: it is created when the HTTP
  connection is opened, and is deleted once it is closed.

* It is the only output connector not created and managed by the user.

* Usage is through a special
  endpoint: [/v0/pipelines/:pipeline_name/egress/table_or_view_name?format=...](/api/subscribe-to-view)

* Specify data output format using URL query parameters
  (e.g., `format=...`, and more depending on format).

The HTTP output connector does not yet support [fault
tolerance](/pipelines/fault-tolerance).

## Output modes

The `send_snapshot` query parameter controls how the connector starts:

* Omitted or `false` (default): Stream only incremental updates.
* `true`: Send a full snapshot of the materialized view before streaming
  incremental updates. The view must be materialized. Each response chunk
  includes a `snapshot` field: `true` for snapshot data, `false` for
  subsequent deltas.

`send_snapshot=true` works whether the pipeline is `Running` or `Paused`:
when the client connects, the snapshot is delivered from the most recent
cached view state, even if the pipeline is paused and no new input is
flowing.

## Example usage

We will subscribe to a stream of updates to the `average_price` view for pipeline `supply-chain-pipeline`.

### curl

Stream incremental updates (default):

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/egress/average_price?format=json'
```

Receive a full snapshot followed by incremental updates:

```bash
curl -i -X 'POST' \
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/egress/average_price?format=json\&send_snapshot=true'
```

### Python (direct API calls)

```python
import requests

api_url = "http://127.0.0.1:8080"
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

* [REST API documentation](/api/subscribe-to-view)
  for the `/egress` endpoint.
