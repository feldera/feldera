-- rule: json_array_len
-- spark: json_array_length(json_str) — length of a JSON array string
-- feldera: CARDINALITY(CAST(PARSE_JSON(json_str) AS VARIANT ARRAY))
CREATE OR REPLACE TEMP VIEW product_tags_v1 AS SELECT id, json_array_length(tag_json) AS tag_count FROM product_tags;
