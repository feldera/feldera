-- rule: from_json_field
-- spark: from_json(json_str, schema) — parse JSON and extract fields
-- feldera: PARSE_JSON(json_str)['field'] + CAST per field
CREATE OR REPLACE TEMP VIEW order_summary_v3 AS
SELECT
  order_num,
  CAST(from_json(details_json, 'customer STRING, quantity INT, amount DOUBLE, date STRING').customer AS STRING) AS cust_name,
  CAST(from_json(details_json, 'customer STRING, quantity INT, amount DOUBLE, date STRING').quantity AS INT) AS qty,
  CAST(from_json(details_json, 'customer STRING, quantity INT, amount DOUBLE, date STRING').amount AS DOUBLE) AS total_amount,
  CAST(from_json(details_json, 'customer STRING, quantity INT, amount DOUBLE, date STRING').date AS STRING) AS order_date
FROM order_records_v3
WHERE CAST(from_json(details_json, 'customer STRING, quantity INT, amount DOUBLE, date STRING').quantity AS INT) > 0;
