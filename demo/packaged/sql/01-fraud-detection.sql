-- Use Case: Real-Time Fraud Detection (fraud-detection)
--
-- Tackle fraud-detection using a real-time feature engineering pipeline in SQL.
--
-- ## How to run
--
-- * Paste this SQL code in the Feldera WebConsole and hit Play ▶️.
-- * In the Change Stream tab, select the `transaction_with_aggregates` view.
-- * You should see feature vectors being produced in real-time as the
--   data generator pushes input changes to the pipeline.
--
-- ## Detailed description
--
-- When it comes to fraud detection, training an accurate ML model is only the
-- beginning. To ensure its effectiveness, you must supply the model with
-- up-to-date features computed from the latest data.
--
-- Feldera's unique incremental query engine makes it an ideal compute platform for
-- fraud detection teams:
--
-- * Expressive: Write complex feature pipelines in SQL
-- * Unified online/offline processing: run the same SQL queries during model
--   training and real-time inference.
-- * Throughput: process millions of records per second on a single host.
-- * Feature freshness: Upon ingesting new events, Feldera outputs new and
--   updated feature vectors within milliseconds.
-- * No online/offline skew: Feldera guarantees identical outputs on batch and
--   streaming inputs.
--
-- In this example we show how to implement two important types of feature
-- queries in Feldera:
--
-- * Data enrichment queries, which combine (or enrich) raw transaction data
--   with data from other sources.
-- * Rolling aggregate queries: foe each event in a time series, compute an
--   aggregate (e.g., count, sum, average, etc.) over a fixed time frame
--  preceding this event.
--
-- See https://feldera.com/fraud_detection for more details.


-- Credit card holders.
CREATE TABLE CUSTOMER (
    cc_num BIGINT NOT NULL PRIMARY KEY, -- Credit card number
    name varchar,                       -- Customer name
    lat DOUBLE,                         -- Customer home address latitude
    long DOUBLE                         -- Customer home address longitude
) WITH (
    'materialized' = 'true',
    -- Configure the random data generator to generate 100000 customer records.
    -- (see https://www.feldera.com/docs/connectors/sources/datagen)
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
          "plan": [{
            "limit": 100000,
            "fields": {
              "name": { "strategy": "name" },
              "cc_num": { "range": [ 100000000000000, 100000000100000 ] },
              "lat": { "strategy": "uniform", "range": [ 25, 50 ] },
              "long": { "strategy": "uniform", "range": [ -126, -67 ] }
            }
          }]
        }
      }
    }]'
);

-- Credit card transactions.
CREATE TABLE TRANSACTION (
    ts TIMESTAMP LATENESS INTERVAL 10 MINUTES, -- Transaction time
    amt DOUBLE,                                -- Transaction amount
    cc_num BIGINT NOT NULL,                    -- Credit card number
    shipping_lat DOUBLE,                       -- Shipping address latitude
    shipping_long DOUBLE,                      -- Shipping address longitude
    FOREIGN KEY (cc_num) REFERENCES CUSTOMER(cc_num)
) WITH (
    'materialized' = 'true',
    -- Configure the random data generator to generate 1M transactions at the rate of 1000 transactions/s.
    'connectors' = '[{
      "transport": {
      "name": "datagen",
        "config": {
          "plan": [{
            "limit": 1000000,
            "rate": 1000,
            "fields": {
              "ts": { "strategy": "increment", "scale": 1000, "range": [1722063600000,2226985200000] },
              "amt": { "strategy": "zipf", "range": [ 1, 10000 ] },
              "cc_num": { "strategy": "uniform", "range": [ 100000000000000, 100000000100000 ] },
              "shipping_lat": { "strategy": "uniform", "range": [ 25, 50 ] },
              "shipping_long": { "strategy": "uniform", "range": [ -126, -67 ] }
            }
          }]
        }
      }
    }]'
);

-- Data enrichment query: left-join the TRANSACTION table with the CUSTOMER table to compute the
-- distance between the shipping address and the customer's home address for each transaction.
CREATE VIEW TRANSACTION_WITH_DISTANCE AS
    SELECT
        t.*,
        ST_DISTANCE(ST_POINT(shipping_long, shipping_lat), ST_POINT(long,lat)) AS distance
    FROM
        TRANSACTION as t
        LEFT JOIN CUSTOMER as c
        ON t.cc_num = c.cc_num;

-- Compute two rolling aggregates over a 1-day time window for each transaction:
-- 1. Average spend per transaction.
-- 2. The number of transactions whose shipping address is more than 50,000 meters away from
--    the card holder's home address.
CREATE VIEW TRANSACTION_WITH_AGGREGATES AS
SELECT
   *,
   AVG(amt) OVER window_1_day as avg_1day,
   SUM(case when distance > 50000 then 1 else 0 end) OVER window_1_day as count_1day
FROM
  TRANSACTION_WITH_DISTANCE
WINDOW window_1_day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 1 DAYS PRECEDING AND CURRENT ROW);
