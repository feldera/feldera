CREATE TABLE demographics (
    cc_num FLOAT64 NOT NULL,
    first STRING,
    gender STRING,
    street STRING,
    city STRING,
    state STRING,
    zip INTEGER,
    lat FLOAT64,
    long FLOAT64,
    city_pop INTEGER,
    job STRING,
    dob STRING
    --dob DATE
);

CREATE TABLE transactions (
    trans_date_trans_time TIMESTAMP NOT NULL,
    cc_num FLOAT64 NOT NULL,
    merchant STRING,
    category STRING,
    amt FLOAT64,
    trans_num STRING,
    unix_time INTEGER NOT NULL,
    merch_lat FLOAT64,
    merch_long FLOAT64,
    is_fraud INTEGER
);

CREATE VIEW features as
    SELECT
        -- DAYOFWEEK(trans_date_trans_time) AS d,
        -- TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,
        "ST_DISTANCE"("ST_POINT"(long,lat), "ST_POINT"(merch_long,merch_lat)) AS distance,
        -- TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,
        AVG(amt) OVER(
            PARTITION BY   CAST(cc_num AS NUMERIC)
            ORDER BY unix_time
            -- 1 week is 604800  seconds
            RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS
        avg_spend_pw,
        AVG(amt) OVER(
            PARTITION BY  CAST(cc_num AS NUMERIC)
            ORDER BY unix_time
            -- 1 month(30 days) is 2592000 seconds
            RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS
        avg_spend_pm,
        COUNT(*) OVER(
            PARTITION BY  CAST(cc_num AS NUMERIC)
            ORDER BY unix_time
            -- 1 day is 86400  seconds
            RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS
        trans_freq_24,
        category,
        amt,
        state,
        job,
        unix_time,
        city_pop,
        merchant,
        is_fraud
    FROM (
        SELECT t1.*, t2.*
               -- , LAG(trans_date_trans_time, 1) OVER (PARTITION BY t1.cc_num  ORDER BY trans_date_trans_time ASC) AS last_txn_date
        FROM  transactions AS t1
        LEFT JOIN  demographics AS t2
        ON t1.cc_num = t2.cc_num);
