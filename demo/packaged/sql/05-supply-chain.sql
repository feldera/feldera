-- Tutorial: Feldera Basics (supply-chain)
--
-- Learn Feldera's key concepts and features using a simple example inspired
-- by supply chain analytics use cases.
--
-- This example accompanies the Feldera Basics tutorial:
-- https://www.feldera.com/docs/tutorials/basics/
--
-- ## How to run
--
-- * Paste this SQL code in the Feldera WebConsole.
-- * In the Change Stream tab, select the `preferred_vendor` view.
-- * Hit Play ▶️.
-- * You should see several output records in the Change Stream.
-- * Use the following `curl` command to update the PRICE table
--   and observe output changes in the Change Stream view:
--
--   ```
--   curl -H "Authorization: Bearer <API-KEY>" -X 'POST'
--   https://try.feldera.com/v0/pipelines/supply-chain/ingress/PRICE?format=json -d '
--   > {"delete": {"part": 1, "vendor": 2, "price": 10000}}
--   > {"insert": {"part": 1, "vendor": 2, "price": 30000}}
--   > {"delete": {"part": 2, "vendor": 1, "price": 15000}}
--   > {"insert": {"part": 2, "vendor": 1, "price": 50000}}
--   > {"insert": {"part": 1, "vendor": 3, "price": 20000}}
--   > {"insert": {"part": 2, "vendor": 3, "price": 11000}}'
--   ```
--
--   (use the account menu in the top right corner of the WebConsole to generate
--   an API key if you don't have one yet).
--
-- ## Detailed description
--
-- In this example we build a simple Feldera pipeline that ingests data about
-- vendors, parts, and prices, and continuously tracks the lowest available
-- price for each part across all vendors.
--
-- It shows how to:
--
-- * Define inputs to Feldera using `CREATE TABLE` statements.
-- * Specify queries against these input tables using `CREATE VIEW` statements.
-- * Configure data sources for each input table
--   (in this example, we use the HTTP GET connector: http://localhost:3000/docs/connectors/sources/http-get)
-- * Send data to the pipeline using the Feldera REST API.
--
-- See https://www.feldera.com/docs/tutorials/basics/ for a detailed
-- description.

-- Vendors.
CREATE TABLE vendor (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR,
    address VARCHAR
) WITH ('connectors' = '[{
    "name": "vendor",
    "transport": {
        "name": "url_input",
        "config": {
            "path": "https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json"
        }
    },
    "format": {
        "name": "json"
    }
}]');

-- Parts.
CREATE TABLE part (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR
) WITH ('connectors' = '[{
    "name": "part",
    "transport": {
        "name": "url_input",
        "config": {
            "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
        }
    },
    "format": {
        "name": "json"
    }
}]');

-- Prices.
CREATE TABLE price (
    part BIGINT NOT NULL,
    vendor BIGINT NOT NULL,
    price DECIMAL
) WITH ('connectors' = '[{
    "name": "tutorial-price-s3",
    "transport": {
        "name": "url_input",
        "config": {
            "path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"
        }
    },
    "format": {
        "name": "json"
    }
}]');

-- Lowest available price for each part across all vendors
CREATE VIEW lowest_price (part, price) AS
    SELECT    part, MIN(price) AS price
    FROM      price
    GROUP BY  part;

-- Lowest available price for each part along with part and vendor details
CREATE VIEW preferred_vendor (
    part_id,
    part_name,
    vendor_id,
    vendor_name,
    price
) AS (
    SELECT
        part.id AS part_id,
        part.name AS part_name,
        vendor.id AS vendor_id,
        vendor.name AS vendor_name,
        price.price
    FROM
        price,
        part,
        vendor,
        lowest_price
    WHERE
        price.price = lowest_price.price AND
        price.part = lowest_price.part AND
        part.id = price.part AND
        vendor.id = price.vendor
);
