-- rule: parse_json_group_by
-- spark: SELECT get_json_object(payload,'$.category') AS cat, COUNT(*) AS cnt FROM t GROUP BY get_json_object(payload,'$.category') — aggregate on JSON field
-- feldera: Use CTE inside CREATE VIEW to pre-parse: CREATE VIEW v AS WITH parsed AS (SELECT *, PARSE_JSON(payload) AS doc FROM t) SELECT CAST(doc['category'] AS VARCHAR) AS cat, COUNT(*) AS cnt FROM parsed GROUP BY CAST(doc['category'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW txn_by_status_v3 AS SELECT get_json_object(metadata, '$.status') AS status, COUNT(*) AS cnt FROM transaction_records_v3 GROUP BY get_json_object(metadata, '$.status');
