-- Product
CREATE TABLE product (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

-- Each item is of a certain product
CREATE TABLE item (
    id INT PRIMARY KEY,
    product_id INT NOT NULL,
    manufactured_at TIMESTAMP NOT NULL,
    bought_at TIMESTAMP NOT NULL,
    sold_at TIMESTAMP
);

-- Total number of products
CREATE VIEW product_count AS
    SELECT COUNT(*) AS num_products FROM product;

-- Total number of items
CREATE VIEW item_count AS
    SELECT COUNT(*) AS num_items FROM item;

-- For each product, count the number of items
CREATE VIEW items_per_product AS
    SELECT product.id AS product_id, product.name AS product_name, COUNT(*) AS num_items
    FROM product LEFT JOIN item ON product.id = item.product_id
    GROUP BY (product.id, product.name);
