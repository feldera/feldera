-- rule: json_array_len
-- spark: json_array_length(json_str) — length of a JSON array string
-- feldera: CARDINALITY(CAST(PARSE_JSON(json_str) AS VARIANT ARRAY))
CREATE TABLE survey_responses (response_id INT, answers STRING);
