-- rule: bit_length
-- spark: BIT_LENGTH(s) — bit length of string
-- feldera: OCTET_LENGTH(s) * 8
CREATE OR REPLACE TEMP VIEW bit_length_v3 AS SELECT id, text_value, notes, BIT_LENGTH(text_value) AS text_bits, BIT_LENGTH(CONCAT(text_value, notes)) AS combined_bits FROM encoded_text_3;
