# Kafka output connector

Feldera can output a stream of changes to a SQL table or view to a Kafka topic.

* The Kafka connector uses **librdkafka** in its implementation.
  [Relevant options supported by it](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
  can be defined in the connector configuration.

## Example usage

We will create a Kafka output connector named `total-sales`.
Kafka broker is located at `example.com:9092` and the topic is `total-sales`.

### curl

```bash
curl -i -X PUT http://localhost:8080/v0/connectors/total-sales \
-H "Authorization: Bearer <API-KEY>" \
-H 'Content-Type: application/json' \
-d '{
  "description": "Kafka output connector for total sales from the book fair",
  "config": {
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
}'
```

### Python (direct API calls)

```python
import requests

api_url = "http://localhost:8080"
headers = { "authorization": f"Bearer <API-KEY>" }

requests.put(
    f"{api_url}/v0/connectors/total-sales", 
    headers=headers,
    json={
      "description": "Kafka output connector for total sales from the book fair",
      "config": {
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
                  "array": False
              }
          }
      }
    }
).raise_for_status()
```

## Additional resources

For more information, see:

* [Kafka input connector](/docs/connectors/sources/kafka#how-to-write-connector-config)
  for examples on how to write the configuration to connect to Kafka brokers (e.g., how to
  specify authentication and encryption).

* [Tutorial section](/docs/tutorials/basics/part3#step-2-create-kafkaredpanda-connectors) which involves
  creating a Kafka output connector.

* Data formats such as [JSON](https://www.feldera.com/docs/api/json) and
  [CSV](https://www.feldera.com/docs/api/csv)

* Overview of Kafka configuration options:
  [librdkafka options](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
