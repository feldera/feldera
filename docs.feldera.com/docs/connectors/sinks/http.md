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

## Query parameters

| Parameter | Values | Default | Effect |
|-----------|--------|---------|--------|
| `format` | `csv`, `json` | `csv` | The data format of the chunks. |
| `backpressure` | `true`, `false` | `false` | With `true`, the connector waits for the client to receive each chunk, so a client that cannot keep up fills the connector's queue and pauses the pipeline, as [`max_queued_records`](/connectors#max_queued_records) describes (100,000 records here, unless a request body sets it), but receives every chunk.  With `false`, the connector drops the chunks that it cannot deliver while the client falls behind, and the pipeline never waits. |
| `send_snapshot` | `true`, `false` | `false` | Send a snapshot of the view before the updates.  See [Output modes](#output-modes). |
| `update_format` (JSON only) | `insert_delete`, `snowflake`, `debezium`, `redis` | `insert_delete` | The encoding of each update.  See [the JSON format](/formats/json). |
| `delimiter` (CSV only) | one ASCII character | `,` | The field delimiter. |

Each JSON chunk holds its updates in a JSON array, so the `array` parameter of
the JSON format is always `true` for this connector.  Instead of query
parameters, the request body may hold a connector configuration in JSON, with
the same `transport` and `format` objects as a connector declared in SQL; the
two cannot be mixed.

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

## Response chunks

The response body is a stream of JSON objects, one per line.  Each object
has a `sequence_number`, counted from 0 for the stream, and a `snapshot`
flag, and carries its updates in `text_data` for CSV or `json_data` for JSON.
A chunk without data either carries `processed_steps`, which says that the
output of every step below that count has been sent, or is a keep-alive that
the connector sends after three seconds without updates.  See
[Waiting for output on an HTTP stream](/connectors/completion-tokens#waiting-for-output-on-an-http-stream)
and the [`Chunk` type](/api/subscribe-to-view).

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
  'http://127.0.0.1:8080/v0/pipelines/supply-chain-pipeline/egress/average_price?format=json&send_snapshot=true'
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
