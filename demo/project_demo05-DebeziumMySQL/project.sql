-- SQL program from the Debezium MySQL tutorial used to demo Feldera
-- Debezium integration.

CREATE TABLE customers (
  id int NOT NULL PRIMARY KEY,
  first_name varchar(255) NOT NULL,
  last_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL
);

CREATE TABLE addresses (
  id int NOT NULL PRIMARY KEY,
  customer_id int NOT NULL,
  street varchar(255) NOT NULL,
  city varchar(255) NOT NULL,
  state varchar(255) NOT NULL,
  zip varchar(255) NOT NULL,
  type varchar(32) NOT NULL
);

CREATE TABLE orders (
  order_number int NOT NULL PRIMARY KEY,
  order_date date NOT NULL,
  purchaser int NOT NULL,
  quantity int NOT NULL,
  product_id int NOT NULL
);

CREATE TABLE products (
  id int NOT NULL PRIMARY KEY,
  name varchar(255) NOT NULL,
  description varchar(512),
  weight real
);

CREATE TABLE products_on_hand (
  product_id int NOT NULL PRIMARY KEY,
  quantity int NOT NULL
);
