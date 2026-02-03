-- Tutorial: Time Series Analysis with Feldera (time-series-tutorial)
--
-- Learn to effectively work with time series data in Feldera.
--
-- This program accompanies the guide to Time Series Analysis with Feldera: https://docs.feldera.com/tutorials/time-series
--
-- Each view definition demonstrates one concept from the guide.
--
-- Example inputs:
-- Insert the following records in the `purchase` table and observe how
-- the views change in response:
--
--    INSERT INTO purchase VALUES(1, '2020-01-01 01:00:00', 10);
--    INSERT INTO purchase VALUES(1, '2020-01-01 02:00:00', 10);
--    INSERT INTO purchase VALUES(1, '2020-01-02 00:00:00', 10);
--    INSERT INTO purchase VALUES(1, '2020-01-02 01:00:00', 10);


-- This table uses two time-series-related annotations:
-- `LATENESS` - updates to this table can arrive no more than 1 hour out of order.
-- `append_only` - records are only inserted to this table, but never deleted.
CREATE TABLE purchase (
   customer_id INT,
   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
   amount BIGINT
) WITH (
    'materialized' = 'true',
    'append_only' = 'true'
);

CREATE TABLE returns (
    customer_id INT,
    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
    amount BIGINT
) WITH (
    'materialized' = 'true'
);

CREATE TABLE customer (
    customer_id INT,
    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAY,
    address VARCHAR
) WITH (
    'materialized' = 'true'
);

-- Daily MAX purchase amount.
CREATE MATERIALIZED VIEW daily_max AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) as d,
    MAX(amount) AS max_amount
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);

-- Daily total purchase amount.
CREATE MATERIALIZED VIEW daily_total AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) as d,
    SUM(amount) AS total
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);

-- Like `daily_total`, but this view uses the 'emit_final' annotation to only
-- produce the final value of the aggregate at the end of each day.
CREATE MATERIALIZED VIEW daily_total_final
WITH ('emit_final' = 'd')
AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) as d,
    SUM(amount) AS total
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);

-- Daily MAX purchase amount computed using tumbling windows.
CREATE MATERIALIZED VIEW daily_max_tumbling AS
SELECT
    window_start,
    MAX(amount)
FROM TABLE(
  TUMBLE(
    "DATA" => TABLE purchase,
    "TIMECOL" => DESCRIPTOR(ts),
    "SIZE" => INTERVAL 1 DAY))
GROUP BY
    window_start;

-- Daily MAX purchase amount computed as a rolling aggregate.
CREATE MATERIALIZED VIEW daily_max_rolling AS
SELECT
    ts,
    amount,
    MAX(amount) OVER window_1_day
FROM purchase
WINDOW window_1_day AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW);

-- Use an OUTER JOIN to compute a daily transaction summary, including daily totals
-- from `purchase` and `returns` tables.
CREATE MATERIALIZED VIEW daily_totals AS
WITH
    purchase_totals AS (
        SELECT
            TIMESTAMP_TRUNC(purchase.ts, DAY) as purchase_date,
            SUM(purchase.amount) as total_purchase_amount
        FROM purchase
        GROUP BY
            TIMESTAMP_TRUNC(purchase.ts, DAY)
    ),
    return_totals AS (
        SELECT
            TIMESTAMP_TRUNC(returns.ts, DAY) as return_date,
            SUM(returns.amount) as total_return_amount
        FROM returns
        GROUP BY
            TIMESTAMP_TRUNC(returns.ts, DAY)
    )
SELECT
    purchase_totals.purchase_date as d,
    purchase_totals.total_purchase_amount,
    return_totals.total_return_amount
FROM
    purchase_totals
    FULL OUTER JOIN
    return_totals
ON
    purchase_totals.purchase_date = return_totals.return_date;

-- Use an ASOF JOIN to extract the customer’s address at the time of
-- purchase from the `customer` table.
CREATE MATERIALIZED VIEW purchase_with_address AS
SELECT
    purchase.ts,
    purchase.customer_id,
    customer.address
FROM purchase
LEFT ASOF JOIN customer MATCH_CONDITION(purchase.ts >= customer.ts)
ON purchase.customer_id = customer.customer_id;

-- Use LAG and LEAD operators to lookup previous and next purchase
-- amounts for each record in the `purchase` table.
CREATE MATERIALIZED VIEW purchase_with_prev_next AS
SELECT
    ts,
    customer_id,
    amount,
    LAG(amount) OVER(PARTITION BY customer_id ORDER BY ts) as previous_amount,
    LEAD(amount) OVER(PARTITION BY customer_id ORDER BY ts) as next_amount
FROM
    purchase;

-- Temporal filter query that uses the current physical time (NOW())
-- to compute transactions made in the last 7 days.
CREATE MATERIALIZED VIEW recent_purchases AS
SELECT * FROM purchase
WHERE
    ts >= NOW() - INTERVAL 7 DAYS;
