-- rule: parse_json_lateral_alias
-- spark: SELECT get_json_object(payload,'$.user_id') AS uid, get_json_object(payload,'$.amount') AS amt FROM t — extract multiple JSON fields (parses JSON multiple times)
-- feldera: SELECT PARSE_JSON(payload) AS v, CAST(v['user_id'] AS VARCHAR) AS uid, CAST(v['amount'] AS DOUBLE) AS amt FROM t — use lateral alias to parse JSON once and reuse
CREATE TABLE transactions_v2 (tx_id BIGINT, json_data STRING);
