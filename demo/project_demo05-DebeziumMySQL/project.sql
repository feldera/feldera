-- SQL program from the Debezium MySQL tutorial used to demo Feldera
-- Debezium integration.

CREATE TABLE customers (
  id int NOT NULL PRIMARY KEY,
  first_name varchar(255) NOT NULL,
  last_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL
) WITH (
    'connectors' = '[{
        "name": "customers",
        "transport": {
            "name": "kafka_input",
            "config": {
                "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                "auto.offset.reset": "earliest",
                "topics": ["inventory.inventory.customers"]
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "debezium",
                "json_flavor": "debezium_mysql"
            }
        }
    }]'
);

CREATE TABLE addresses (
  id int NOT NULL PRIMARY KEY,
  customer_id int NOT NULL,
  street varchar(255) NOT NULL,
  city varchar(255) NOT NULL,
  state varchar(255) NOT NULL,
  zip varchar(255) NOT NULL,
  type varchar(32) NOT NULL
) WITH (
    'connectors' = '[{
        "name": "addresses",
        "transport": {
            "name": "kafka_input",
            "config": {
                "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                "auto.offset.reset": "earliest",
                "topics": ["inventory.inventory.addresses"]
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "debezium",
                "json_flavor": "debezium_mysql"
            }
        }
    }]'
);

CREATE TABLE orders (
  order_number int NOT NULL PRIMARY KEY,
  order_date date NOT NULL,
  purchaser int NOT NULL,
  quantity int NOT NULL,
  product_id int NOT NULL
) WITH (
    'connectors' = '[{
        "name": "orders",
        "transport": {
            "name": "kafka_input",
            "config": {
                "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                "auto.offset.reset": "earliest",
                "topics": ["inventory.inventory.orders"]
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "debezium",
                "json_flavor": "debezium_mysql"
            }
        }
    }]'
);

CREATE TABLE products (
  id int NOT NULL PRIMARY KEY,
  name varchar(255) NOT NULL,
  description varchar(512),
  weight real
) WITH (
    'connectors' = '[{
        "name": "products",
        "transport": {
            "name": "kafka_input",
            "config": {
                "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                "auto.offset.reset": "earliest",
                "topics": ["inventory.inventory.products"]
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "debezium",
                "json_flavor": "debezium_mysql"
            }
        }
    }]'
);

CREATE TABLE products_on_hand (
  product_id int NOT NULL PRIMARY KEY,
  quantity int NOT NULL
) WITH (
    'connectors' = '[{
        "name": "products_on_hand",
        "transport": {
            "name": "kafka_input",
            "config": {
                "bootstrap.servers": "[REPLACE-BOOTSTRAP-SERVERS]",
                "auto.offset.reset": "earliest",
                "topics": ["inventory.inventory.products_on_hand"]
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "debezium",
                "json_flavor": "debezium_mysql"
            }
        }
    }]'
);
