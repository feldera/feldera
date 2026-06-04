-- Tutorial: Building a spreadsheet using Feldera (spreadsheet)
--
-- A copy of the billion-cell spreadsheet demo from https://github.com/feldera/techdemo-spreadsheet.
--

-- Given a cell value as a formula (e.g., =A0+B0), and a context with cell values
-- referenced in the formula, returns the computed value of the cell
CREATE FUNCTION cell_value(cell VARCHAR(64), mentions_ids BIGINT ARRAY, mentions_values VARCHAR(64) ARRAY) RETURNS VARCHAR(64);

-- Given a cell value e.g., =A0+B0, returns an array of cell ids that were mentioned in the formula
CREATE FUNCTION mentions(cell VARCHAR(64)) RETURNS BIGINT ARRAY;

-- Forward declaration of spreadsheet view
DECLARE RECURSIVE VIEW spreadsheet_view (
     id BIGINT NOT NULL,
     background INTEGER NOT NULL,
     raw_value VARCHAR(64) NOT NULL,
     computed_value VARCHAR(64)
);

-- Raw spreadsheet cell data coming from backend/user, updates
-- are inserted as new entries with newer timestamps
CREATE TABLE spreadsheet_data (
     id BIGINT NOT NULL,
     ip VARCHAR(45) NOT NULL,
     ts TIMESTAMP NOT NULL,
     raw_value VARCHAR(64) NOT NULL,
     background INTEGER NOT NULL
) WITH (
      'materialized' = 'true',
      'connectors' = '[{
        "name": "data",
        "transport": {
            "name": "datagen",
            "config": {
                "workers": 1,
                "plan": [{
                    "limit": 23,
                    "fields": {
                        "id": { "values": [1039999974, 0, 1, 2, 12, 14, 40, 66, 92, 118, 170, 196, 222, 13, 65, 91, 117, 15, 41, 67, 93, 119, 39, 144] },
                        "ip": { "values": ["0"] },
                        "raw_value": { "values": ["42", "=A39999999", "=A0", "=A0+B0", "Reference", "Functions", "=ABS(-1)", "=AVERAGE(1,2,3,1,2,3)", "={1,2,3}+{1,2,3}", "=SUM(1,2,3)", "=PRODUCT(ABS(1),2*1, 3,4*1)", "=RIGHT(\"apple\", 3)", "=LEFT(\"apple\", 3)", "Logic", "=2>=1", "=OR(1>1,1<>1)", "=AND(\"test\",\"True\", 1, true)", "Datetime", "2019-03-01T02:00:00.000Z", "2019-08-30T02:00:00.000Z", "=DAYS(P1, P2)", "=P1+5", "=XOR(0,1)", "=IF(TRUE,1,0)"] },
                        "background": { "strategy": "uniform", "range": [0, 1] }
                    }
                }]
            }
        }
    }]'
      );

-- Get the latest cell value for the spreadsheet.
-- (By finding the one with the highest `ts` for a given `id`)
CREATE VIEW latest_cells AS WITH
       max_ts_per_cell AS (
           SELECT
               id,
               MAX(ts) AS max_ts
           FROM
               spreadsheet_data
           GROUP BY
               id
       )
   SELECT
       s.id,
       s.raw_value,
       s.background,
       -- The append with null is silly but crucial to ensure that the
       -- cross join in `latest_cells_with_mention` returns all cells
       -- not just those that reference another cell
       ARRAY_APPEND(mentions(s.raw_value), null) AS mentioned_cell_ids
   FROM
       spreadsheet_data s
           JOIN max_ts_per_cell mt ON s.id = mt.id AND s.ts = mt.max_ts;

-- List all mentioned ids per latest cell
CREATE VIEW latest_cells_with_mentions AS
SELECT
    s.id,
    s.raw_value,
    s.background,
    m.mentioned_id
FROM
    latest_cells s, UNNEST(s.mentioned_cell_ids) AS m(mentioned_id);

-- Like latest_cells_with_mentions, but enrich it with values of mentioned cells
CREATE LOCAL VIEW mentions_with_values AS
SELECT
    m.id,
    m.raw_value,
    m.background,
    m.mentioned_id,
    sv.computed_value AS mentioned_value
FROM
    latest_cells_with_mentions m
        LEFT JOIN
    spreadsheet_view sv ON m.mentioned_id = sv.id;

-- We aggregate mentioned values and ids back into arrays
CREATE LOCAL VIEW mentions_aggregated AS
SELECT
    id,
    raw_value,
    background,
    ARRAY_AGG(mentioned_id) AS mentions_ids,
    ARRAY_AGG(mentioned_value) AS mentions_values
FROM
    mentions_with_values
GROUP BY
    id,
    raw_value,
    background;

-- Calculate the final spreadsheet by executing the UDF for the formula
CREATE MATERIALIZED VIEW spreadsheet_view AS
SELECT
    id,
    background,
    raw_value,
    cell_value(raw_value, mentions_ids, mentions_values) AS computed_value
FROM
    mentions_aggregated;

-- Figure out which IPs currently reached their API limit
CREATE MATERIALIZED VIEW api_limit_reached AS
SELECT
    ip
FROM
    spreadsheet_data
WHERE
    ts >= NOW() - INTERVAL 60 MINUTES
GROUP BY
    ip
HAVING
    COUNT(*) > 100;

-- Compute statistics
CREATE MATERIALIZED VIEW spreadsheet_statistics AS
WITH filled_total AS (
    SELECT
        COUNT(DISTINCT id) AS filled_total
    FROM
        spreadsheet_data
),
filled_this_hour AS (
    SELECT
        COUNT(*) AS filled_this_hour
    FROM
        spreadsheet_data
    WHERE
        ts >= NOW() - INTERVAL 1 HOUR
),
filled_today AS (
    SELECT
        COUNT(*) AS filled_today
    FROM
        spreadsheet_data
    WHERE
        ts >= NOW() - INTERVAL 1 DAY
),
filled_this_week AS (
    SELECT
        COUNT(*) AS filled_this_week
    FROM
        spreadsheet_data
    WHERE
        ts >= NOW() - INTERVAL 1 WEEK
),
currently_active_users AS (
    SELECT
        COUNT(DISTINCT ip) AS currently_active_users
    FROM
        spreadsheet_data
    WHERE
        ts >= NOW() - INTERVAL 5 MINUTE
)
SELECT
    (SELECT filled_total FROM filled_total) AS filled_total,
    (SELECT filled_this_hour FROM filled_this_hour) AS filled_this_hour,
    (SELECT filled_today FROM filled_today) AS filled_today,
    (SELECT filled_this_week FROM filled_this_week) AS filled_this_week,
    (SELECT currently_active_users FROM currently_active_users) AS currently_active_users;
