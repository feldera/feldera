CREATE TABLE json_test_table (
    id INT NOT NULL PRIMARY KEY,
    bi BIGINT,
    s VARCHAR,
    d DOUBLE,
    f REAL,
    i INT,
    b BOOLEAN,
    ts TIMESTAMP,
    dt DATE,
    json VARCHAR,
    uuid VARCHAR
) with (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "kafka_input",
      "config": {
        "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
        "auto.offset.reset": "earliest",
        "topics": ["json.test_schema.test_table"]
      }
    },
    "format": {
        "name": "json",
        "config": {
            "update_format": "debezium",
            "json_flavor": "debezium_postgres"
        }
    }
}]');

CREATE TABLE avro_test_table (
    id INT NOT NULL PRIMARY KEY,
    bi BIGINT,
    s VARCHAR,
    d DOUBLE,
    f REAL,
    i INT,
    b BOOLEAN,
    ts TIMESTAMP,
    dt DATE,
    json VARCHAR,
    uuid VARCHAR
) with (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "kafka_input",
      "config": {
        "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
        "auto.offset.reset": "earliest",
        "topics": ["avro.test_schema.test_table"]
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "registry_urls": ["[REPLACE-REGISTRY-URL]"],
        "update_format": "debezium"
      }
    }
}]');