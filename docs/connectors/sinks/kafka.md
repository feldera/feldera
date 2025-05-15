# Kafka output connector

Feldera can output a stream of changes to a SQL table or view to a Kafka topic.

The Kafka output connector supports [fault
tolerance](/pipelines/fault-tolerance).

The Kafka connector uses **librdkafka** in its implementation and
supports [relevant options for
consumers](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

## Example usage

We will create a Kafka output connector named `total-sales`.
Kafka broker is located at `example.com:9092` and the topic is `total-sales`.

```sql
CREATE VIEW V
WITH (
   'connectors' = '[
    {
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "example.com:9092",
              "auto.offset.reset": "earliest",
              "topic": "total-sales"
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
   ]'
)
AS ...
```

### Authentication & Encryption

#### SSL with PEM keys

```json
"config": {
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topic": "book-fair-sales",
    "security.protocol": "SSL",
    "ssl.ca.pem": "-----BEGIN CERTIFICATE-----TOPSECRET0\n-----END CERTIFICATE-----\n",
    "ssl.key.pem": "-----BEGIN CERTIFICATE-----TOPSECRET1\n-----END CERTIFICATE-----\n",
    "ssl.certificate.pem": "-----BEGIN CERTIFICATE-----TOPSECRET2\n-----END CERTIFICATE-----\n",
}
```

PEM-encoded certificates can be passed directly in the configuration using
`ssl.*.pem` keys.

#### Multiple certificates

librdkafka only accepts multiple certificates when provided via
`ssl.certificate.location` keys (file paths) rather than directly
with `ssl.certificate.pem`.

This is documented in
[librdkafka issue #3225](https://github.com/confluentinc/librdkafka/issues/3225).

To work around this limitation, Feldera handles PEM-encoded certificates and
keys by:

1. Storing the value passed to `ssl.certificate.pem` into a file.
2. Naming the file using the SHA256 hash of the data.
3. Replacing the `ssl.certificate.pem` configuration option with
   `ssl.certificate.location` option that points to the newly saved file.

**Example:**

The updated configuration would look like:

```json
"config": {
    ...,
    "ssl.certificate.location": "path/to/certificate.pem"
}
```

> :warning: If both `ssl.certificate.pem` and `ssl.certificate.location` are set
the latter will be overwritten.

### Connecting to AWS MSK with IAM SASL

Example of writing data to AWS MSK with IAM SASL.

:::important
- AWS credentials must either be set as Environment Variables or present in `~/.aws/credentials`.
- Ensure that the defined output topic either exists in AWS MSK or, automatic topic creation is enabled.
- `sasl.mechanism` must be set to `OAUTHBEARER`.
- `security.protocol` must be set to `SASL_SSL`.

Other protocols and mechanisms aren't supported.
:::

```sql

CREATE VIEW OUTPUT
WITH (
   'connectors' = '[
    {
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "broker-1.kafka.region.amazonaws.com:9098,broker-2.kafka.region.amazonaws.com:9098",
              "sasl.mechanism": "OAUTHBEARER",
              "security.protocol": "SASL_SSL",
              "topic": "<TOPIC>"
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
   ]'
) as select * from INPUT;
```


## Additional resources

For more information, see:

* [Kafka input connector](/connectors/sources/kafka#how-to-write-connector-config)
  for examples on how to write the configuration to connect to Kafka brokers (e.g., how to
  specify authentication and encryption).

* [Tutorial section](/tutorials/basics/part3#step-2-configure-kafkaredpanda-connectors) which involves
  creating a Kafka output connector.

* Data formats such as [JSON](/formats/json) and
  [CSV](/formats/csv)

* Overview of Kafka configuration options:
  [librdkafka options](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
