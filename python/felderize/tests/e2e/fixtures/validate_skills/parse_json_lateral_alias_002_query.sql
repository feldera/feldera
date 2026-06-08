-- rule: parse_json_lateral_alias
-- spark: SELECT get_json_object(payload,'$.user_id') AS uid, get_json_object(payload,'$.amount') AS amt FROM t — extract multiple JSON fields (parses JSON multiple times)
-- feldera: SELECT PARSE_JSON(payload) AS v, CAST(v['user_id'] AS VARCHAR) AS uid, CAST(v['amount'] AS DOUBLE) AS amt FROM t — use lateral alias to parse JSON once and reuse
CREATE OR REPLACE TEMP VIEW transactions_extracted_v2 AS SELECT get_json_object(json_data, '$.user_id') AS user_identifier, get_json_object(json_data, '$.amount') AS transaction_amount, get_json_object(json_data, '$.currency') AS curr FROM transactions_v2;
