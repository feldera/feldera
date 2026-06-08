-- rule: get_json_numeric
-- spark: get_json_object(json_str, '$.numeric_field') — extract a NUMERIC-typed JSON field (integer or float) and return as string
-- feldera: CAST(CAST(PARSE_JSON(json_str)['field'] AS DOUBLE) AS VARCHAR) — CRITICAL [GBD-JSON-CAST]: CAST(variant AS VARCHAR) returns NULL for numeric JSON values. Must double-cast: cast to numeric type first (BIGINT for integers, DOUBLE for decimals), then to VARCHAR. Do NOT use CAST(PARSE_JSON(...)['field'] AS VARCHAR) directly.
CREATE OR REPLACE TEMP VIEW financial_amount_extract AS SELECT transaction_id, category, get_json_object(amount_json, '$.value') AS extracted_amount FROM financial_records WHERE category IN ('debit', 'credit');
