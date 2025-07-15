-- Use Case: Real-Time Feature Engineering (feature-engineering)
--
-- Compute complex ML features over an unbounded data stream using bounded memory.
--

/*
We implement several typical ML features in SQL and evaluate them over a real-time data stream:
- Data enrichment using ASOF JOIN
- Rolling aggregate over a 30-day time window
- Global aggregates over the entire event history

As you browse through the code, note how complex features are implemented by composing simple
SQL views.

Try it!
▶️ Start the demo to generate and process a stream of 1 billion random credit card transactions.
📈 Check the Performance tab: as Feldera processes millions of incoming transactions,
   its memory footprint remains steady around the 2GB mark.
   See https://docs.feldera.com/tutorials/time-series to learn how Feldera evaluates many complex
   SQL queries using bounded memory.
💪 Feldera is strongly consistent: its output correctly reflects all inputs received so far.
   Scroll down to the 'user_stats' view to learn more.
*/

-- Customers.
CREATE TABLE customer (
    id BIGINT NOT NULL,
    name varchar,
    state VARCHAR,
    -- Lateness annotation: customer records cannot arrive more than 7 days out of order.
    ts TIMESTAMP LATENESS INTERVAL 7 DAYS
) WITH (
    -- Generate 10000 customer records.
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
          "plan": [{
            "limit": 10000,
            "fields": {
              "name": { "strategy": "name" },
              "state": {"strategy": "state_abbr"},
              "ts": { "range": ["2023-01-01T00:00:00Z", "2024-01-01T00:00:00Z"], "strategy": "zipf" }
            }
          },
          {
            "limit": 1,
            "fields": {"ts": { "values": ["2080-01-01T00:00:00Z"] } }
          }]
        }
      }
    }]'
);

-- Credit card transactions.
CREATE TABLE transaction (
    -- Lateness annotation: transactions cannot arrive more than 1 day out of order.
    ts TIMESTAMP LATENESS INTERVAL 1 DAYS,
    amt DECIMAL(38, 2),
    customer_id BIGINT NOT NULL,
    state VARCHAR
) WITH (
    -- Generate a stream of 1 billion credit card transactions.
    -- (remove the '"rate": 10000' line to run the data generator at full throttle).
    'connectors' = '[{
      "transport": {
      "name": "datagen",
        "config": {
          "plan": [{
            "limit": 1000000000,
            "rate": 10000,
            "fields": {
              "ts": { "strategy": "increment", "scale": 1000, "range": ["2024-01-01T00:00:00Z", "2074-01-01T00:00:00Z"] },
              "amt": { "strategy": "zipf", "range": [ 1, 10000 ] },
              "state": {"strategy": "state_abbr"},
              "customer_id": { "range": [ 0, 10000 ] }
            }
          }]
        }
      }
    }]'
);

-- Data enrichment:
-- * Use ASOF JOIN to find the most recent customer record for each transaction.
-- * Compute 'out_of_state' flag, which indicates that the transaction was performed outside
--   of the customer's home state.
CREATE VIEW enriched_transaction AS
SELECT
    transaction.*,
    (transaction.state != customer.state) AS out_of_state
FROM
    transaction LEFT ASOF JOIN customer
    MATCH_CONDITION ( transaction.ts >= customer.ts )
    ON transaction.customer_id = customer.id;

-- Rolling aggregation: Compute the number of out-of-state transactions in the last 30 days for each transaction.
CREATE VIEW transaction_with_history AS
SELECT
    *,
    SUM(case when out_of_state then 1 else 0 end) OVER window_30_day as out_of_state_count
FROM
    enriched_transaction
WINDOW window_30_day AS (PARTITION BY customer_id ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW);

-- Flag out-of-state transactions, but only if there have been fewer than 5 such transactions
-- over the last 30 days.
CREATE VIEW red_transactions AS
SELECT
    *
FROM
    transaction_with_history
WHERE
    out_of_state AND out_of_state_count < 5;

CREATE VIEW green_transactions AS
SELECT
    *
FROM
    transaction_with_history
WHERE
    (NOT out_of_state) OR out_of_state_count >= 5;

-- Compute total amount across all transactions, as well as total counts of red and green transactions
-- for each user.
--
-- # Strong consistency
--
-- The Feldera SQL engine is strongly consistent: the output of a pipeline at any point in time
-- is identical to the output produced by evaluating all views on the current snapshot of all input
-- tables.
--
-- The following view illustrates strong consistency. It combines data from three separate views
-- ('enriched_transactions', 'red_transactions', and 'green_transactions') in a single view.  In
-- a strongly consistent system, the sum of red and green transactions should always be equal to the
-- total amount across all transactions.  To check that this is the case, use the 'Ad-hoc query' tab to
-- run the following query at any time while the pipeline is running:
--
-- select * from user_stats where total_amt - red_amt != green_amt
--
-- You should _always_ see empty output!
CREATE MATERIALIZED VIEW user_stats AS
WITH
    total_amt AS (select customer_id, sum(amt) AS total_amt FROM enriched_transaction GROUP BY customer_id),
    red_amt AS (select customer_id, sum(amt) AS red_amt FROM red_transactions GROUP BY customer_id),
    green_amt AS (select customer_id, sum(amt) AS green_amt FROM green_transactions GROUP BY customer_id)
SELECT
    total_amt.customer_id,
    COALESCE(red_amt, 0) as red_amt,
    COALESCE(green_amt, 0) as green_amt,
    COALESCE(total_amt, 0) as total_amt
FROM
    total_amt
    LEFT JOIN red_amt ON total_amt.customer_id = red_amt.customer_id
    LEFT JOIN green_amt ON total_amt.customer_id = green_amt.customer_id;
