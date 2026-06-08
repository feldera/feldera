-- rule: bit_length
-- spark: BIT_LENGTH(s) — bit length of string
-- feldera: OCTET_LENGTH(s) * 8
CREATE OR REPLACE TEMP VIEW bit_length_v2 AS SELECT record_id, message, description, BIT_LENGTH(message) AS msg_bits, BIT_LENGTH(description) AS desc_bits FROM string_data_2;
