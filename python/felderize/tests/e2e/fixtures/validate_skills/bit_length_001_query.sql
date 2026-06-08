-- rule: bit_length
-- spark: BIT_LENGTH(s) — bit length of string
-- feldera: OCTET_LENGTH(s) * 8
CREATE OR REPLACE TEMP VIEW bit_length_v1 AS SELECT id, content, BIT_LENGTH(content) AS bit_count FROM text_samples_1;
