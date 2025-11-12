-- Example input
CREATE TABLE example (
    id INT NOT NULL PRIMARY KEY
) WITH (
    'connectors' = '[
        {
            "name": "kafka-0",
            "transport": {
                "name": "kafka_input",
                "config": {
                    "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                    "topics": ["simple_count_input"],
                    "auto.offset.reset": "earliest"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    "array": false
                }
            }
        },
        {
            "name": "kafka-1",
            "transport": {
                "name": "kafka_input",
                "config": {
                    "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                    "topics": ["simple_count_input"],
                    "auto.offset.reset": "earliest"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    "array": false
                }
            }
        },
        {
            "name": "kafka-2",
            "transport": {
                "name": "kafka_input",
                "config": {
                    "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                    "topics": ["simple_count_input"],
                    "auto.offset.reset": "earliest"
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
);

-- Example output
CREATE VIEW example_count WITH (
    'connectors' = '[
        {
            "name": "kafka-1",
            "transport": {
                "name": "kafka_output",
                "config": {
                    "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                    "topic": "simple_count_output"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    "array": true
                }
            }
        },
        {
            "name": "kafka-2",
            "transport": {
                "name": "kafka_output",
                "config": {
                    "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                    "topic": "simple_count_output"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    "array": true
                }
            }
        },
        {
            "name": "kafka-3",
            "transport": {
                "name": "kafka_output",
                "config": {
                    "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                    "topic": "simple_count_output"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    "array": true
                }
            }
        }
    ]'
) AS (
    SELECT COUNT(*) AS num_rows FROM example
);
