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
    dob DATE
);

CREATE TABLE transactions (
    trans_date_trans_time TIMESTAMP NOT NULL,
    cc_num FLOAT64 NOT NULL,
    merchant STRING,
    category STRING,
    amt FLOAT64,
    trans_num STRING,
    unix_time INTEGER,
    merch_lat FLOAT64,
    merch_long FLOAT64,
    is_fraud INTEGER
);

CREATE VIEW transactions_with_demographics as 
    SELECT
        transactions.trans_date_trans_time,
        transactions.cc_num,
        demographics.first,
        demographics.city
    FROM
        transactions JOIN demographics
        ON transactions.cc_num = demographics.cc_num;
