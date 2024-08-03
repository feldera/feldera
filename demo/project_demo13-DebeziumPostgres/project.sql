-- SQL program from the Debezium MySQL tutorial used to demo Feldera
-- Debezium integration.

CREATE TABLE customers (
  id int NOT NULL PRIMARY KEY,
  first_name varchar(255) NOT NULL,
  last_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL
) with
  'materialized' = 'true',
  'connectors' = '[{
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
}]';

CREATE TABLE orders (
  id int NOT NULL,
  order_date date NOT NULL,
  purchaser int NOT NULL,
  quantity int NOT NULL,
  product_id int NOT NULL
) with (
  'materialized' = 'true',
  'connectors' = '[{
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
  }]');

CREATE TABLE products (
  id int NOT NULL PRIMARY KEY,
  name varchar(255) NOT NULL,
  description varchar(512),
  weight real
) with (
  'materialized' = 'true',
     'connectors' = '[{
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
  }]');

CREATE TABLE products_on_hand (
  product_id int NOT NULL PRIMARY KEY,
  quantity int NOT NULL
) with (
  'materialized' = 'true',
     'connectors' = '[{
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
   }]');
