# Synchronous Processing with Completion Tokens

Completion tokens enable synchronous processing on top of Feldera’s asynchronous pipelines, allowing
users to precisely track when a specific set of inputs has been fully processed by the pipeline.

## Background

Feldera pipelines process data **asynchronously**. While the incremental SQL engine is evaluating
earlier inputs, input connectors continue ingesting data into internal buffers, and output connectors
write prior results to external sinks.

![Architecture of a Feldera Pipeline](../pipelines/pipeline_architecture.png)

However, the SQL engine itself is **synchronous** and **strongly consistent**. It processes inputs in
discrete transactions—each transaction: ingests a set of records from input connectors, processes them
to completion and outputs **all** resulting changes to the output views before moving on to the next
transaction.

This enables Feldera to determine precisely when a specific set of inputs has been fully processed,
from ingest all the way to writing results to data sinks. Completion tokens expose this capability via
a simple API.

## Completion tokens

A completion token is an opaque handle representing a specific point in the input stream from one connector.
The client can use the completion token to check whether all inputs ingested before the token was issued have
been fully processed, and the resulting outputs have been written to all data sinks attached to the pipeline.

Completion tokens can be used to implement synchronous processing as follows:

1. Submit data to the pipeline.
2. Generate a completion token.
3. Wait for processing to complete by checking the token's status.

Completion tokens have several important properties:

* A completion token is tied to a specific input connector and only tracks inputs
 received from this connector.

* The pipeline can continue process new inputs, including from the same input connector, while the client
 is waiting on a completion token.

* The client can generate any number of completion tokens for the same or different connectors to
 track the status of multiple inputs at the same time.

This enables efficient, overlapping execution: clients can push additional inputs while
waiting for earlier inputs to be processed.

## How to generate a completion token

There are two ways to generate a completion token, depending on the type of the input connector:

1. The [HTTP input connector](/connectors/sources/http) returns a token
 in response to every [`/ingress`](/api/insert-data) request.
 This token tracks completion for the data ingested as part of this request.

2. For all other input connectors, use the
 [`/completion_token`](/api/get-completion-token)
 endpoint to generate a token. This token tracks all inputs ingested by the connector
 before the endpoint was invoked.

## Checking completion status

To check the processing status of a token, call the
[`/completion_status`](/api/check-completion-status)
endpoint with the token.  The endpoint returns
`"complete"` if the data associated with the token has been fully processed and all outputs derived from
it have been written to all connected data sinks.  It returns `"inprogress"` otherwise.
The response also carries `step`: the number of steps the pipeline completes
before the token is complete.

## Waiting for output on an HTTP stream

For an [HTTP output connector](/connectors/sinks/http), "written to the
sink" means that the pipeline has queued the output for the stream that an
[`/egress`](/api/subscribe-to-view) request opened.  The data can still be in
transit when the token completes, so a client that reads the stream cannot
conclude from `"complete"` alone that it has received the output.

After the output of each step, the pipeline sends an empty chunk whose
`processed_steps` field counts the steps whose output the stream has
delivered, in the same unit as the `step` of `/completion_status`:

```json
{"sequence_number": 12, "snapshot": false, "processed_steps": 7}
```

A client that has received a chunk with `processed_steps` at or above the
token's `step` is guaranteed to have received every update for the token.
The chunk comes with the commit of the step's transaction (in continuous
mode, each step runs in a transaction of its own; see
[Transactions](/pipelines/transactions)), so the steps of an explicit
transaction are reported by one chunk after the commit's output.
Without [`backpressure`](/connectors/sinks/http#query-parameters), the
connector drops chunks when the client falls behind, step chunks included, so
a client that waits this way should open the stream with `backpressure=true`.
A stream reports only the steps that end after it was opened, so a token that
completed before that is covered by the first chunk of the next step.

In the Python SDK, `OutputHandler.wait_for_token(token)` blocks until a
listener from `Pipeline.listen()` has buffered every change for the token.

## Example

We will use the Supply Chain pipeline from the [Feldera Basics Tutorial](/tutorials/basics/) as an example.
Select Feldera Basics tutorial in the "Use cases and tutorials" section of the WebConsole in your
local Feldera deployment or in the [Feldera online sandbox](https://try.feldera.com/) to create the
pipeline. We will use the following table from the tutorial in this example:

```sql
CREATE TABLE price (
    part BIGINT NOT NULL,
    vendor BIGINT NOT NULL,
    price DECIMAL
) WITH ('connectors' = '[{
    "name": "tutorial-price-s3",
    "transport": {
        "name": "url_input",
        "config": {
            "path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"
        }
    },
    "format": {
        "name": "json"
    }
}]');
```

### Step 1. Push data via HTTP

  ```shell
  curl -s -X 'POST' 'http://127.0.0.1:8080/v0/pipelines/supply-chain/ingress/PRICE?format=json' -d '
  {"insert": {"part": 1, "vendor": 2, "price": 10000}}
  {"insert": {"part": 2, "vendor": 1, "price": 15000}}
  {"insert": {"part": 3, "vendor": 3, "price": 9000}}'| jq
  ```

  Sample response (completion token):

  ```json
  {
    "token": "eyJ1IjoiMDE5NmIxMjAtNjIwMy03YjkwLWI2YmQtMTY4OTBkZjE1ZTI4IiwiZSI6MywiYyI6M30="
  }
  ```

### Step 2. Generate a token for the URL input connector

  Let us generate another completion token for the URL input connector attached to the table.

  ```shell
  curl -s -X 'GET' 'http://127.0.0.1:8080/v0/pipelines/supply-chain/tables/PRICE/connectors/tutorial-price-s3/completion_token' | jq
  ```

  > Note that this endpoint requires the user to explicitly assign a name to the connector (`"name": "tutorial-price-s3"`)
  in the SQL table declaration.

  This returns another completion token:

  ```json
  {
    "token": "eyJ1IjoiMDE5NmIxMjAtNjIwMy03YjkwLWI2YmQtMTY4OTBkZjE1ZTI4IiwiZSI6MSwiYyI6M30="
  }
  ```

### Step 3. Check the status of both tokens

  ```shell
  curl -s -X 'GET' 'http://127.0.0.1:8080/v0/pipelines/supply-chain/completion_status?token=eyJ1IjoiMDE5NmIxMjAtNjIwMy03YjkwLWI2YmQtMTY4OTBkZjE1ZTI4IiwiZSI6MywiYyI6M30=' | jq
  ```

  ```json
  {
    "status": "complete"
  }
  ```

  ```shell
  curl -s -X 'GET' 'http://127.0.0.1:8080/v0/pipelines/supply-chain/completion_status?token=eyJ1IjoiMDE5NmIxMjAtNjIwMy03YjkwLWI2YmQtMTY4OTBkZjE1ZTI4IiwiZSI6MSwiYyI6M30=' | jq
  ```

  ```json
  {
    "status": "complete"
  }
  ```
