# Kafka output connector

Feldera can output a stream of changes to a SQL table or view to a Kafka topic.

* The Kafka connector uses **librdkafka** in its implementation.
  [Relevant options supported by it](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
  can be defined in the connector configuration.

## Example usage

We will create a Kafka output connector named `total-sales`.
Kafka broker is located at `example.com:9092` and the topic is `total-sales`.

```sql
CREATE VIEW V AS ...
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
