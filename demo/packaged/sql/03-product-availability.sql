-- Use Case: Product Availability (product-availability)
--
-- Track product availability across warehouses as inventory is continuously
-- updated with changes from a generator source.
--
-- ## How to run
--
-- * Paste this SQL code in the Feldera WebConsole and hit Play ▶️.
-- * In the Change Stream tab, select the `top3_warehouse_volume` view.
-- * You should see the list of the top-3 warehouses change in real-time.
--
-- ## Detailed description
--
-- This simple example illustrates incremental computation in action.
---
-- The input tables represent the availability of various inventory items across
-- multiple warehouses. We define analytical queries over these tables to calculate
-- metrics, such as the total availability of individual items across all warehouses.
--
-- A random data generator continuously produces updates to the tables, simulating
-- changes in inventory levels. As Feldera processes these updates, it generates a
-- stream of changes to the query results in real-time.

-- Warehouse
CREATE TABLE warehouse (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    address VARCHAR NOT NULL
) with (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 10,
                    "fields": {
                        "name": { "values": ["Warhouse no. 1", "Warehouse no. 2", "Warehouse no. 3", "Warehouse no. 4", "Warehouse no. 5", "Warehouse no. 6", "Warehouse no. 7", "Warehouse no. 8", "Warehouse no. 9", "Warehouse no. 10"] },
                        "address": { "strategy": "street_name" }
                    }
                }]
            }
        }
    }]'
);

-- Product
CREATE TABLE product (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    mass DOUBLE NOT NULL,
    volume DOUBLE NOT NULL
) with (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 20,
                    "fields": {
                        "name": { "strategy": "buzzword" },
                        "mass": { "strategy": "uniform", "range": [0, 1000]},
                        "volume": { "strategy": "uniform", "range": [0, 1000]}
                    }
                }]
            }
        }
    }]'
);

-- Each warehouse stores products
CREATE TABLE storage (
    warehouse_id INT NOT NULL FOREIGN KEY REFERENCES warehouse(id),
    product_id INT NOT NULL FOREIGN KEY REFERENCES product(id),
    num_available INT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (warehouse_id, product_id)
) with (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "rate": 200,
                    "fields": {
                        "warehouse_id": { "strategy": "uniform", "range": [0, 10]},
                        "product_id": { "strategy": "uniform", "range": [0, 20]},
                        "num_available": { "strategy": "zipf", "range": [0, 100]},
                        "updated_at": { "strategy": "uniform", "range": [1722648000000, 4102444800000]}
                    }
                }]
            }
        }
    }]'
);

-- How much of each product is stored
CREATE MATERIALIZED VIEW product_stored AS
(
    SELECT   product.id,
             product.name,
             SUM(storage.num_available) AS num_available,
             SUM(storage.num_available * product.mass) AS total_mass,
             SUM(storage.num_available * product.volume) AS total_volume
    FROM     product
             LEFT JOIN storage ON product.id = storage.product_id
             LEFT JOIN warehouse ON storage.warehouse_id = warehouse.id
    GROUP BY (product.id, product.name)
);

-- How much each warehouse has stored
CREATE MATERIALIZED VIEW warehouse_stored AS
(
    SELECT   warehouse.id,
             warehouse.name,
             SUM(storage.num_available) AS num_available,
             SUM(storage.num_available * product.mass) AS total_mass,
             SUM(storage.num_available * product.volume) AS total_volume
    FROM     warehouse
             LEFT JOIN storage ON warehouse.id = storage.warehouse_id
             LEFT JOIN product ON storage.product_id = product.id
    GROUP BY (warehouse.id, warehouse.name)
);

-- Top 3 warehouse according to stored mass
CREATE MATERIALIZED VIEW top_3_warehouse_mass AS
(
    SELECT   warehouse_stored.id,
             warehouse_stored.name,
             warehouse_stored.total_mass
    FROM     warehouse_stored
    ORDER BY warehouse_stored.total_mass DESC
    LIMIT    3
);

-- Top 3 warehouse according to stored volume
CREATE MATERIALIZED VIEW top_3_warehouse_volume AS
(
    SELECT   warehouse_stored.id,
             warehouse_stored.name,
             warehouse_stored.total_volume
    FROM     warehouse_stored
    ORDER BY warehouse_stored.total_volume DESC
    LIMIT    3
);

-- Availability stats across all products
CREATE MATERIALIZED VIEW product_availability AS
(
    SELECT COUNT(*) AS num_product,
           MIN(product_stored.num_available) AS min_availability,
           AVG(product_stored.num_available) AS avg_availability,
           MAX(product_stored.num_available) AS max_availability,
           SUM(product_stored.num_available) AS sum_availability
    FROM   product_stored
);

-- Total number of warehouses
CREATE MATERIALIZED VIEW num_warehouse AS
SELECT COUNT(*) AS num_warehouse
FROM   warehouse;

-- Total number of products
CREATE MATERIALIZED VIEW num_product AS
SELECT COUNT(*) AS num_product
FROM   product;

-- Total number of storage entries
CREATE MATERIALIZED VIEW num_storage AS
SELECT COUNT(*) AS num_storage
FROM   storage;
