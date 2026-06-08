-- rule: bit_length
-- spark: BIT_LENGTH(s) — bit length of string
-- feldera: OCTET_LENGTH(s) * 8
CREATE TABLE string_data_2 (record_id INT, message STRING, description STRING);
