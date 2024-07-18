-- Warehouse
CREATE TABLE warehouse (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    address VARCHAR NOT NULL
) WITH (
    'materialized' = 'true'
);

-- Product
CREATE TABLE product (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    mass DOUBLE NOT NULL,
    volume DOUBLE NOT NULL
) WITH (
    'materialized' = 'true'
);

-- Each warehouse stores products
CREATE TABLE storage (
    warehouse_id INT FOREIGN KEY REFERENCES warehouse(id),
    product_id INT FOREIGN KEY REFERENCES product(id),
    num_available INT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (warehouse_id, product_id)
) WITH (
    'materialized' = 'true'
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
