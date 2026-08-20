-- Keeping an ML Model in the Loop
--
-- Companion SQL for
-- https://docs.feldera.com/use_cases/model_scoring/model_scoring
--
-- This pipeline needs a model server to be useful

SET FELDERA_IGNORE_WARNING_UNUSED_COLUMN = 1;

-- ---------------------------------------------------------------------------
-- Data inputs. Every one of them is an unbounded stream

-- Credit card transactions
-- The event stream the model predicts on
CREATE TABLE transaction (
    trans_id BIGINT NOT NULL,
    cc_num BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAYS,
    amount DECIMAL(10, 2) NOT NULL,
    merchant_category VARCHAR NOT NULL
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "transaction",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 6,
                    "fields": {
                        "trans_id": { "values": [1, 2, 3, 4, 5, 6] },
                        "cc_num": { "values": [1001, 1001, 1001, 1002, 1002, 1002] },
                        "ts": { "values": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z"] },
                        "amount": { "values": [2000.00, 8500.00, 9000.00, 1000.00, 4000.00, 3750.00] },
                        "merchant_category": { "values": ["grocery", "electronics", "grocery", "electronics", "electronics", "travel"] }
                    }
                }]
            }
        }
    }]'
);

-- Credit card holders
CREATE TABLE cardholder (
    cc_num BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL LATENESS INTERVAL 7 DAYS,
    zip INT NOT NULL,
    credit_limit DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (cc_num, ts)
) WITH (
    'connectors' = '[{
        "name": "cardholder",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 2,
                    "fields": {
                        "cc_num": { "values": [1001, 1002] },
                        "ts": { "values": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"] },
                        "zip": { "values": [94105, 10001] },
                        "credit_limit": { "values": [10000.00, 5000.00] }
                    }
                }]
            }
        }
    }]'
);

-----------------------------------------------------------------------------
-- Compute features

CREATE LOCAL VIEW recent_transaction AS
SELECT * FROM transaction WHERE ts > NOW() - INTERVAL 30 DAYS;

CREATE LOCAL VIEW features AS
SELECT
    t.trans_id,
    t.cc_num,
    t.ts,
    t.amount,
    t.merchant_category,
    c.zip,
    c.credit_limit,
    -- Share of the cardholder's credit limit this transaction consumes.
    CAST(t.amount * 100 / c.credit_limit AS DECIMAL(10, 2)) AS pct_of_limit,
    -- Rolling sum of the spends over the preceding week
    CAST(AVG(t.amount) OVER window_7_day AS DECIMAL(10, 2)) AS avg_amount_7d,
    COUNT(*) OVER window_7_day AS txn_count_7d
FROM recent_transaction t
-- ASOF JOIN picks the version of the "cardholder" in effect when the transaction
-- happened, not the last version
LEFT ASOF JOIN cardholder c
MATCH_CONDITION ( t.ts >= c.ts )
ON t.cc_num = c.cc_num
WHERE c.credit_limit IS NOT NULL
WINDOW window_7_day AS (
    PARTITION BY t.cc_num
    ORDER BY t.ts
    RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW);

-- 'MATERIALIZED' is for debugging only
CREATE MATERIALIZED VIEW fingerprinted_features AS
SELECT
    -- Every feature the model reads must appear in the fingerprint;
    -- a feature left out would let a stale prediction survive a change to it. When
    -- you add a column to `features`, add it here as well.
    MD5(CAST(f.trans_id AS VARCHAR) || '|' ||
        CAST(f.ts AS VARCHAR) || '|' ||
        CAST(f.amount AS VARCHAR) || '|' ||
        f.merchant_category || '|' ||
        CAST(f.zip AS VARCHAR) || '|' ||
        CAST(f.credit_limit AS VARCHAR) || '|' ||
        CAST(f.pct_of_limit AS VARCHAR) || '|' ||
        CAST(f.avg_amount_7d AS VARCHAR) || '|' ||
        CAST(f.txn_count_7d AS VARCHAR)) AS request_fingerprint,
    f.*
FROM features f
-- How far back we are willing to predict.
WHERE f.ts > NOW() - INTERVAL 30 DAYS;

------------------------------------------------------
-- Analyze predictions

-- For some transactions, the ground truth, confirmed generally long after the transaction
-- has occurred.
CREATE TABLE confirmed_fraud_label (
    trans_id BIGINT NOT NULL,
    -- Transaction time
    ts TIMESTAMP NOT NULL LATENESS INTERVAL 90 DAYS,
    is_fraud BOOLEAN NOT NULL,
    PRIMARY KEY (trans_id, ts)
) WITH (
    'connectors' = '[{
        "name": "confirmed_fraud_label",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 2,
                    "fields": {
                        "trans_id": { "values": [2, 5] },
                        "ts": { "values": ["2024-01-02T00:00:00Z", "2024-01-02T00:00:00Z"] },
                        "is_fraud": { "values": [true, true] }
                    }
                }]
            }
        }
    }]'
);

-- Model outputs, written by the model server.
-- The model server never deletes from this table, it only inserts new predictions.
CREATE TABLE model_prediction (
    event_time TIMESTAMP NOT NULL LATENESS INTERVAL 90 DAYS,
    request_fingerprint VARCHAR NOT NULL,
    trans_id BIGINT NOT NULL,
    fraud_probability DECIMAL(5, 4) NOT NULL,
    -- Wall-clock stamp from the model server
    predicted_at TIMESTAMP NOT NULL,
    PRIMARY KEY (event_time, trans_id)
);

-- Recent predictions
CREATE LOCAL VIEW live_prediction AS
SELECT * FROM model_prediction WHERE event_time > NOW() - INTERVAL 90 DAYS;

-- Inputs to the model server
-- This view should declare an output connector, most likely Kafka.
-- This demo instead has the model server subscribe to the view using
-- HTTP, which needs no connector declaration.
CREATE MATERIALIZED VIEW unpredicted_features AS
SELECT r.*
FROM fingerprinted_features r
WHERE NOT EXISTS (
    -- Exclude predictions which have answers already
    SELECT 1 FROM live_prediction p
    WHERE p.request_fingerprint = r.request_fingerprint AND p.event_time = r.ts);

-- Debugging view only, could be removed in production
CREATE MATERIALIZED VIEW predicted_transaction AS
SELECT
    r.trans_id,
    r.ts,
    r.amount,
    r.merchant_category,
    r.pct_of_limit,
    p.fraud_probability,
    p.fraud_probability >= 0.5 AS predicted_fraud
FROM fingerprinted_features r
JOIN live_prediction p
  ON r.request_fingerprint = p.request_fingerprint AND r.ts = p.event_time;

-- Compute quality of model predictions
CREATE LOCAL VIEW model_confusion AS
SELECT
    COUNT(*) AS scored,
    -- 'is_fraud' = NULL is considered "no fraud".
    SUM(CASE WHEN s.fraud_probability >= 0.5 AND COALESCE(l.is_fraud, FALSE)
             THEN 1 ELSE 0 END) AS true_positive,
    SUM(CASE WHEN s.fraud_probability >= 0.5 AND NOT COALESCE(l.is_fraud, FALSE)
             THEN 1 ELSE 0 END) AS false_positive,
    SUM(CASE WHEN s.fraud_probability < 0.5 AND COALESCE(l.is_fraud, FALSE)
             THEN 1 ELSE 0 END) AS false_negative
FROM live_prediction s
LEFT JOIN confirmed_fraud_label l
  ON s.trans_id = l.trans_id AND s.event_time = l.ts;

-- The model's score, higher is better
CREATE MATERIALIZED VIEW model_score AS
SELECT
    scored,
    true_positive,
    false_positive,
    false_negative,
    DIV_NULL(CAST(true_positive AS DECIMAL(12, 6)),
             true_positive + false_positive) AS precision_score,
    DIV_NULL(CAST(true_positive AS DECIMAL(12, 6)),
             true_positive + false_negative) AS recall_score
FROM model_confusion;
