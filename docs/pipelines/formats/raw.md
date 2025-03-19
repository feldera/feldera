# Raw Format

:::note
This page describes configuration options specific to the raw data format.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

The **raw** data format can be used to configure an input connector to pass
raw, unparsed, data buffers from the [transport connector](/connectors/sources) to the SQL program.
Normally the byte stream received from the transport connector is parsed using the specified data [format](/formats),
e.g., [JSON](/formats/json) or [Avro](/formats/avro). The parser converts the raw byte stream into a stream
of SQL table records.  In some cases it is preferable to pass the raw bytes to the SQL program unmodified and
perform all parsing in SQL. In particular this is useful for:

* **Custom data formats:** If the required data format is not supported by built-in parsers, raw data can be
  ingested into a SQL table and decoded using a [UDF](/sql/udf).

* **Parallelized parsing**: Parsing large volumes of incoming data can become a performance bottleneck.
  Normally parsing is performed by the input connector outside of the SQL runtime.
  Moving parsing into the SQL program enables it to leverage the parallelism in the Feldera
  SQL runtime.

The raw format is currently only supported for input connectors.

Note that the raw format supports only inserting records into the table and does not support deletions.
This limitation exists because the raw data stream lacks the metadata to differentiate between inserts and deletes.

## Configuring raw input connector

In order to ingest raw data:

1. Declare a table with a single column of type `VARBINARY [NOT NUL]` or `VARCHAR [NOT NULL]`.  In the latter case, the connector
   will parse input data as a UTF-8 string and will fail to parse chunks that are not valid UTF-8.

2. Configure an input connector with the format name `raw`.  The connector currently supports a single configuration
   option, `mode`, which can be set to:
   * `blob` (default) - ingest the entire data chunk received from the transport connector as a single SQL row.
     For message-oriented transports, such as Kafka or Pub/Sub, an input chunk corresponds to a message.
     For file-based transports, e.g., the [URL](/connectors/sources/http-get) connector or
     the [S3](/connectors/sources/s3) connector, a chunk represents an entire file or object.
   * `lines` - split the input byte stream on the new line character (`\n`) and ingest each line as a separate SQL row.

:::note

When using the `blob` mode with a file-oriented transport connector such as S3, the entire file or object is ingested
as a single record.  This can require a lot of memory when reading large files.

:::

### Example

The following example shows a table with an input connector configured to ingest raw data from a URL line-by-line.

```sql
create table raw_table(
    data varchar
) with (
    'connectors' = '[{
        "format": {
            "name": "raw",
            "config": {
                "mode": "lines"
            }
        },
        "transport": {
            "name": "url_input",
            "config": {
                "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
            }
        }
    }]'
);
```


## Ingesting raw data via HTTP

You can also push raw data to a pipeline via [HTTP](/connectors/sources/http) by specifying
`format=json` and, optionally, `mode=lines` in the request URL:

### Example

Create a pipeline called `my_pipeline` with the following table declaration:

```sql
create table raw_table(
    data varchar
);
```

Insert two records with values `hello` and `world`:

```bash
curl -i -X 'POST' \
  http://localhost:8080/v0/pipelines/my_pipeline/ingress/raw_table?format=raw&mode=lines \
  -d 'hello
  world'
```
